-- =============================================================
-- 时序数据库存储模型 (MySQL 8.0)
-- 设计要点:
--   1. metric_data 主表: 按天 RANGE 分区 (TO_DAYS), 分区键 ts
--   2. 主键 (metric_id, ts, id) 保证分区键包含在主键中
--   3. 覆盖索引加速 (metric_id, ts) 范围扫描
--   4. metric_data_hourly 预聚合表: 后台任务定期降采样, 查询大时间范围直接命中
--   5. 指标业务状态 active/silent/archived: 活跃参与查询检测, 静默只采集, 归档拒写并可清理
--   6. 数据过期: 定时 EVENT 每日 DROP 最老分区并追加新分区 (避免 DELETE 产生碎片)
-- =============================================================

CREATE DATABASE IF NOT EXISTS tsdb DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
USE tsdb;

-- 指标元数据表
CREATE TABLE IF NOT EXISTS metrics (
    id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    name        VARCHAR(64)  NOT NULL COMMENT '指标名, 如 cpu.usage',
    instance    VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '实例/主机标识',
    unit        VARCHAR(16)  NOT NULL DEFAULT '' COMMENT '单位',
    description VARCHAR(255) NOT NULL DEFAULT '',
    status      ENUM('active', 'silent', 'archived') NOT NULL DEFAULT 'active' COMMENT '业务状态:活跃/静默/归档',
    version     INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '状态乐观锁版本',
    status_updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '状态最后变更时间',
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_name_instance (name, instance),
    KEY idx_status_name (status, name, instance)
) ENGINE=InnoDB COMMENT='指标元数据';

-- ---------------------------------------------------------------
-- 原始时序数据表: 按天分区
-- 分区键必须包含在所有唯一键中 => 主键 = (metric_id, ts, id)
-- ts 使用 DATETIME(3) 毫秒精度; 分区表达式 TO_DAYS(ts)
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metric_data (
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    metric_id  BIGINT UNSIGNED NOT NULL,
    ts         DATETIME(3)     NOT NULL COMMENT '采样时间(毫秒)',
    value      DOUBLE          NOT NULL,
    PRIMARY KEY (id, metric_id, ts),
    KEY idx_metric_ts (metric_id, ts)
) ENGINE=InnoDB
  COMMENT='原始时序数据(按天分区)'
PARTITION BY RANGE (TO_DAYS(ts)) (
    PARTITION p_meta VALUES LESS THAN (TO_DAYS('2020-01-01')) ENGINE=InnoDB
);

-- ---------------------------------------------------------------
-- 小时级预聚合表(降采样结果): 查询大时间跨度时避免扫描原始数据
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metric_data_hourly (
    metric_id   BIGINT UNSIGNED NOT NULL,
    bucket_ts   DATETIME        NOT NULL COMMENT '小时桶起始时间',
    avg_value   DOUBLE NOT NULL,
    min_value   DOUBLE NOT NULL,
    max_value   DOUBLE NOT NULL,
    sum_value   DOUBLE NOT NULL,
    point_count INT UNSIGNED NOT NULL,
    PRIMARY KEY (metric_id, bucket_ts)
) ENGINE=InnoDB COMMENT='小时级预聚合(降采样)';

-- ---------------------------------------------------------------
-- 存储过程: 分区维护
--   p_add_partition: 追加未来 N 天的分区
--   p_drop_old_partitions: 删除 retention_days 之前的分区(数据过期)
-- ---------------------------------------------------------------
DELIMITER $$

DROP PROCEDURE IF EXISTS p_add_partition $$
CREATE PROCEDURE p_add_partition(IN days_ahead INT)
BEGIN
    DECLARE i INT DEFAULT 0;
    DECLARE done INT DEFAULT 0;
    DECLARE p_name VARCHAR(16);
    DECLARE p_date DATE;
    DECLARE max_less_than BIGINT DEFAULT 0;

    -- 当前最大分区边界
    SELECT COALESCE(MAX(CAST(PARTITION_DESCRIPTION AS UNSIGNED)), 0)
      INTO max_less_than
      FROM INFORMATION_SCHEMA.PARTITIONS
     WHERE TABLE_SCHEMA = 'tsdb' AND TABLE_NAME = 'metric_data'
       AND PARTITION_NAME IS NOT NULL;

    WHILE i < days_ahead DO
        SET p_date = CURDATE() + INTERVAL (i + 1) DAY;
        SET p_name = DATE_FORMAT(p_date, 'p%Y%m%d');
        -- 仅当该分区边界大于现有最大边界时才添加
        IF TO_DAYS(p_date) > max_less_than THEN
            SET @sql = CONCAT('ALTER TABLE metric_data ADD PARTITION (PARTITION ',
                              p_name, ' VALUES LESS THAN (TO_DAYS(\'', p_date, '\')))');
            PREPARE stmt FROM @sql;
            EXECUTE stmt;
            DEALLOCATE PREPARE stmt;
        END IF;
        SET i = i + 1;
    END WHILE;
END $$

DROP PROCEDURE IF EXISTS p_drop_old_partitions $$
CREATE PROCEDURE p_drop_old_partitions(IN retention_days INT)
BEGIN
    DECLARE done INT DEFAULT 0;
    DECLARE p_name VARCHAR(16);
    DECLARE cutoff DATE;

    DECLARE cur CURSOR FOR
        SELECT PARTITION_NAME
          FROM INFORMATION_SCHEMA.PARTITIONS
         WHERE TABLE_SCHEMA = 'tsdb' AND TABLE_NAME = 'metric_data'
           AND PARTITION_NAME IS NOT NULL
           AND PARTITION_NAME != 'p_meta'
           AND CAST(PARTITION_DESCRIPTION AS UNSIGNED) < TO_DAYS(CURDATE() - INTERVAL retention_days DAY);
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO p_name;
        IF done THEN LEAVE read_loop; END IF;
        SET @sql = CONCAT('ALTER TABLE metric_data DROP PARTITION ', p_name);
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END LOOP;
    CLOSE cur;
END $$

-- ---------------------------------------------------------------
-- 存储过程: 小时级降采样(增量聚合上一小时的数据)
-- ---------------------------------------------------------------
DROP PROCEDURE IF EXISTS p_downsample_hour $$
CREATE PROCEDURE p_downsample_hour(IN hour_start DATETIME)
BEGIN
    -- 活跃和静默指标仍参与预聚合; 归档指标不再产生新的预聚合数据。
    -- 静默预聚合结果会在查询入口按 metrics.status='active' 被排除。
    INSERT INTO metric_data_hourly (metric_id, bucket_ts, avg_value, min_value, max_value, sum_value, point_count)
    SELECT d.metric_id,
           hour_start,
           AVG(d.value), MIN(d.value), MAX(d.value), SUM(d.value), COUNT(*)
      FROM metric_data d
      JOIN metrics m ON m.id = d.metric_id
     WHERE d.ts >= hour_start AND d.ts < hour_start + INTERVAL 1 HOUR
       AND m.status IN ('active', 'silent')
     GROUP BY d.metric_id
    ON DUPLICATE KEY UPDATE
        avg_value   = VALUES(avg_value),
        min_value   = VALUES(min_value),
        max_value   = VALUES(max_value),
        sum_value   = VALUES(sum_value),
        point_count = VALUES(point_count);
END $$

-- ---------------------------------------------------------------
-- 定时事件: 数据过期策略 + 分区自动维护 + 降采样
-- (事件体含多语句, 需保持 $$ 分隔符)
-- ---------------------------------------------------------------
DROP EVENT IF EXISTS ev_partition_maintenance $$
CREATE EVENT ev_partition_maintenance
    ON SCHEDULE EVERY 1 DAY STARTS (CURRENT_DATE + INTERVAL 1 DAY + INTERVAL 5 MINUTE)
    ON COMPLETION PRESERVE
DO
BEGIN
    CALL p_add_partition(7);        -- 始终保持未来 7 天分区
    CALL p_drop_old_partitions(30); -- 原始数据保留 30 天
END $$

DROP EVENT IF EXISTS ev_downsample $$
CREATE EVENT ev_downsample
    ON SCHEDULE EVERY 1 HOUR STARTS (DATE_FORMAT(NOW() + INTERVAL 1 HOUR, '%Y-%m-%d %H:00:05'))
    ON COMPLETION PRESERVE
DO
    CALL p_downsample_hour(DATE_FORMAT(NOW() - INTERVAL 1 HOUR, '%Y-%m-%d %H:00:00')) $$

DELIMITER ;

SET GLOBAL event_scheduler = ON;

-- 初始化: 立即创建未来 7 天的分区, 无需手动调用
CALL p_add_partition(7);
