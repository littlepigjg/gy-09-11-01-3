# 时序数据存储与可视化平台

基于 **MySQL 分区表 + Python(FastAPI) + Vue3(ECharts)** 的时序数据库服务。接收 CPU、内存等传感器/监控指标，提供高吞吐写入、降采样聚合查询、异常点检测与实时可视化，全部服务通过 Docker Compose 一键编排，**克隆即可运行**。

## 功能特性

- **高吞吐写入**：批量 `executemany` + 连接池 + 单事务提交，实测约 2.8 万点/秒
- **高效时序存储**：MySQL 按天 RANGE 分区，复合索引 `(metric_id, ts)`，查询自动分区裁剪
- **数据过期策略**：定时事件每日 `DROP PARTITION`（原始数据保留 30 天），无碎片、秒级回收
- **降采样聚合**：SQL 侧时间桶聚合（min/max/avg/sum），桶大小按时间跨度自动对齐（1s~1d）
- **预聚合加速**：小时级物化表，跨度 >7 天查询自动路由，30 天查询毫秒级返回
- **异常点检测**：滑动窗口 Z-Score 算法，曲线上红色高亮标注
- **实时可视化**：实时曲线、多指标对比、时间范围/聚合方式切换、图表缩放

## 快速开始

### 环境要求

仅需安装：

- [Docker](https://docs.docker.com/engine/install/) 20.10+
- [Docker Compose](https://docs.docker.com/compose/install/) v2+

无需在本机安装 Python、Node 或 MySQL。

### 一键启动

```bash
# 1. 克隆项目
git clone <your-repo-url> tsdb
cd tsdb

# 2. 构建镜像并后台启动全部服务（首次约需几分钟拉取基础镜像）
docker compose up -d --build

# 3. 查看启动状态（等待 tsdb-mysql 与 tsdb-backend 变为 healthy）
docker compose ps
```

启动后约 1 分钟（MySQL 首次初始化建表、模拟器回填 48 小时历史数据），即可访问：

| 服务 | 地址 | 说明 |
| --- | --- | --- |
| **监控页面** | http://localhost/ | Vue3 + ECharts 可视化（80 端口） |
| **后端 API 文档** | http://localhost:8000/docs | Swagger 交互式接口文档 |
| **MySQL** | `localhost:3306` | root / root，库名 `tsdb` |

> 模拟器会在数据库为空时自动回填 48 小时历史数据，随后每 2 秒持续写入实时数据；容器重启不会重复回填。

### 停止与清理

```bash
# 停止服务（保留数据）
docker compose down

# 停止并删除数据卷（彻底清空，恢复到首次启动状态）
docker compose down -v
```

## 目录结构

```
tsdb/
├── docker-compose.yml      # 一键编排：MySQL + 后端 + 模拟器 + 前端
├── schema.sql              # 数据库模型（分区表/索引/存储过程/定时事件）
├── README.md
├── backend/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py             # FastAPI：写入/聚合/降采样/异常检测
│   └── simulator.py        # 传感器数据模拟器（回填 + 实时）
└── frontend/
    ├── Dockerfile          # 多阶段构建：Node 构建 + Nginx 托管
    ├── nginx.conf          # SPA 托管 + /api 反向代理
    ├── vite.config.js
    └── src/
        ├── App.vue         # 实时曲线/多指标对比/异常标注
        └── ...
```

## 服务架构

```
                         ┌──────────────────────────────┐
   浏览器  ───────────►   │ frontend (Nginx :80)          │
                         │  静态资源 + /api 反向代理       │
                         └───────────────┬──────────────┘
                                         │
                         ┌───────────────▼──────────────┐
                         │ backend (FastAPI :8000)       │
                         │  批量写入 / 聚合 / 降采样 / 异常 │
                         └───────────────┬──────────────┘
                          写入/查询       │
              ┌──────────────┐    ┌──────▼───────────────┐
              │ simulator    │──► │ mysql :3306          │
              │ 回填+实时     │    │  按天分区/索引/事件    │
              └──────────────┘    └──────────────────────┘
```

## API 速览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/write` | 批量写入数据点 |
| GET | `/api/metrics` | 指标列表与数据点统计 |
| GET | `/api/query` | 聚合 + 降采样查询（`metrics/start/end/agg/bucket`） |
| GET | `/api/latest` | 最近窗口原始点（实时曲线） |
| GET | `/api/anomalies` | 滑动窗口 Z-Score 异常点检测 |
| GET | `/api/health` | 健康检查 |

写入示例：

```bash
curl -X POST http://localhost:8000/api/write \
  -H 'Content-Type: application/json' \
  -d '{"points":[{"metric":"cpu.usage","instance":"host-1","value":42.5}]}'
```

聚合查询示例（时间戳为 Unix 秒）：

```bash
curl "http://localhost:8000/api/query?metrics=cpu.usage,mem.usage&start=1788800000&end=1788900000&agg=avg"
```

## 核心设计

**时序索引与分区**：`metric_data` 表 `PARTITION BY RANGE (TO_DAYS(ts))` 按天分区，主键 `(id, metric_id, ts)` 配合复合索引 `(metric_id, ts)`；时间范围查询仅扫描相关分区（分区裁剪），EXPLAIN 验证只命中单天分区。

**数据过期**：MySQL 事件 `ev_partition_maintenance` 每日凌晨自动追加未来 7 天分区并 DROP 30 天前的旧分区；相比逐行 DELETE 无碎片、几乎零成本。

**降采样两级体系**：

1. 在线桶聚合：`GROUP BY FLOOR(UNIX_TIMESTAMP(ts)/bucket)`，桶大小按跨度自动选择。
2. 预聚合表 `metric_data_hourly`：事件每小时增量聚合；大跨度查询自动改查预聚合表，avg 以 `SUM(sum)/SUM(count)` 加权保证准确性。

**异常检测**：对每个数据点用其前 N 个点（默认 20）计算均值与标准差，`|z| > 阈值（默认 3）`判定为异常并返回坐标，前端以红色散点叠加在曲线上。

## 本地开发（可选）

不使用 Docker 时可分别启动各组件：

```bash
# 后端（需要本机 MySQL，配置通过环境变量覆盖）
cd backend
pip install -r requirements.txt
DB_HOST=127.0.0.1 DB_PASSWORD=root uvicorn main:app --reload

# 数据模拟器
python simulator.py auto 48     # 库为空时回填 48h，再实时写入

# 前端（开发服务器，自动代理 /api 到 8000）
cd frontend
npm install
npm run dev                     # http://localhost:5173
```

后端支持的环境变量：`DB_HOST` `DB_PORT` `DB_USER` `DB_PASSWORD` `DB_NAME`；模拟器支持 `API_URL`。

## 常见问题

**Q：80 / 3306 / 8000 端口被占用？**
修改 `docker-compose.yml` 中各服务的 `ports` 映射，例如将前端改为 `"8080:80"`。

**Q：拉取基础镜像超时？**
配置 Docker 镜像加速器后重试。编辑 `/etc/docker/daemon.json`：

```json
{
  "registry-mirrors": ["https://docker.1ms.run", "https://docker.xuanyuan.me"]
}
```

重启 Docker：`sudo systemctl restart docker`。

**Q：构建时 BuildKit 联网校验元数据超时？**
可使用经典构建器：`DOCKER_BUILDKIT=0 docker compose build`。

**Q：想修改数据保留期？**
编辑 `schema.sql` 中事件调用 `CALL p_drop_old_partitions(30)` 的天数，重新初始化数据卷即可生效。
