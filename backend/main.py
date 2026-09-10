# =============================================================
# 时序数据库服务 - Python 后端
# 技术栈: FastAPI + PyMySQL(连接池) + 原生SQL聚合
# 核心能力:
#   1. 高吞吐批量写入 (executemany + 事务批提交)
#   2. 聚合查询 min/max/avg/sum (SQL 侧完成, 避免拉取原始数据)
#   3. 降采样: 固定桶大小时间窗口聚合 (LTTB 风格桶聚合)
#   4. 自动路由: 大时间跨度查询命中小时级预聚合表
#   5. 异常点检测: 基于滑动窗口 Z-Score
# =============================================================
import math
import os
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Optional

import pymysql
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# ---------------------------------------------------------------
# 数据库连接池 (配置全部来自环境变量, 便于容器化部署)
# ---------------------------------------------------------------
DB_CONFIG = dict(
    host=os.getenv("DB_HOST", "127.0.0.1"),
    port=int(os.getenv("DB_PORT", "3306")),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASSWORD", "root"),
    database=os.getenv("DB_NAME", "tsdb"),
    charset="utf8mb4",
    autocommit=False,
    cursorclass=pymysql.cursors.DictCursor,
)


class ConnectionPool:
    """简单的阻塞式连接池, 复用连接降低高并发写入时的握手开销。"""

    def __init__(self, size: int = 8):
        self._pool: list[pymysql.Connection] = []
        self._size = size
        self._lock = __import__("threading").Lock()

    def _create(self) -> pymysql.Connection:
        return pymysql.connect(**DB_CONFIG)

    @contextmanager
    def acquire(self):
        conn = None
        with self._lock:
            if self._pool:
                conn = self._pool.pop()
        if conn is None:
            conn = self._create()
        try:
            conn.ping(reconnect=True)
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            try:
                with self._lock:
                    if len(self._pool) < self._size:
                        self._pool.append(conn)
                    else:
                        conn.close()
            except Exception:
                pass


pool = ConnectionPool(size=10)

app = FastAPI(title="时序数据服务", version="1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------
# 健康检查 (供容器编排 healthcheck 使用)
# ---------------------------------------------------------------
@app.get("/api/health")
def health():
    with pool.acquire() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1")
    return {"status": "ok"}


# ---------------------------------------------------------------
# 请求模型
# ---------------------------------------------------------------
class DataPoint(BaseModel):
    metric: str = Field(..., description="指标名, 如 cpu.usage")
    instance: str = Field("", description="实例标识")
    ts: Optional[float] = Field(None, description="Unix时间戳(秒), 缺省取服务器当前时间")
    value: float


class BatchWriteRequest(BaseModel):
    points: list[DataPoint]


# ---------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------
def _get_metric_id(cursor, name: str, instance: str) -> int:
    """获取或创建指标ID (upsert)。"""
    cursor.execute(
        "INSERT INTO metrics (name, instance) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)",
        (name, instance),
    )
    return cursor.lastrowid


def _resolve_metric_ids(cursor, names: list[str], instance: str = "") -> dict[str, list[int]]:
    """批量解析指标名 -> id 列表。
    instance 为空时跨实例聚合, 一个指标名可能对应多个 id。"""
    fmt = ",".join(["%s"] * len(names))
    if instance:
        cursor.execute(
            f"SELECT id, name FROM metrics WHERE instance=%s AND name IN ({fmt})",
            [instance, *names],
        )
    else:
        cursor.execute(f"SELECT id, name FROM metrics WHERE name IN ({fmt})", names)
    result: dict[str, list[int]] = {}
    for row in cursor.fetchall():
        result.setdefault(row["name"], []).append(row["id"])
    return result


# ---------------------------------------------------------------
# API: 写入
# ---------------------------------------------------------------
@app.post("/api/write")
def write_points(req: BatchWriteRequest):
    """
    高吞吐批量写入。
    优化点:
      - 单事务 + executemany 批量插入, 减少 round-trip 和 redo log 刷盘次数
      - 指标元数据 upsert 与数据写入同事务
    """
    if not req.points:
        return {"inserted": 0}
    t0 = time.perf_counter()
    now = time.time()
    with pool.acquire() as conn, conn.cursor() as cur:
        # 1. 解析所有指标ID (按 (name, instance) 去重)
        metric_ids: dict[tuple[str, str], int] = {}
        for p in req.points:
            key = (p.metric, p.instance)
            if key not in metric_ids:
                metric_ids[key] = _get_metric_id(cur, p.metric, p.instance)
        # 2. 批量插入
        rows = [
            (metric_ids[(p.metric, p.instance)],
             datetime.fromtimestamp(p.ts if p.ts is not None else now),
             p.value)
            for p in req.points
        ]
        cur.executemany(
            "INSERT INTO metric_data (metric_id, ts, value) VALUES (%s, %s, %s)",
            rows,
        )
    elapsed = (time.perf_counter() - t0) * 1000
    return {"inserted": len(rows), "elapsed_ms": round(elapsed, 2)}


# ---------------------------------------------------------------
# API: 指标列表
# ---------------------------------------------------------------
@app.get("/api/metrics")
def list_metrics():
    with pool.acquire() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT m.id, m.name, m.instance, m.unit, "
            "       (SELECT COUNT(*) FROM metric_data d WHERE d.metric_id = m.id) AS points "
            "FROM metrics m ORDER BY m.name"
        )
        return cur.fetchall()


# ---------------------------------------------------------------
# API: 聚合查询 (min/max/avg/sum) + 降采样
# ---------------------------------------------------------------
# 降采样桶大小映射: 根据查询时间跨度自动选择, 保证返回点数适中
def _auto_bucket_seconds(start: float, end: float, target_points: int = 500) -> int:
    span = end - start
    bucket = max(1, math.ceil(span / target_points))
    # 对齐到常用刻度, 便于阅读
    for step in (1, 5, 10, 30, 60, 300, 600, 1800, 3600, 21600, 86400):
        if bucket <= step:
            return step
    return 86400


@app.get("/api/query")
def query_metrics(
    metrics: str = Query(..., description="逗号分隔的指标名"),
    instance: str = Query(""),
    start: float = Query(..., description="起始Unix时间戳(秒)"),
    end: float = Query(..., description="结束Unix时间戳(秒)"),
    agg: str = Query("avg", pattern="^(min|max|avg|sum)$"),
    bucket: Optional[int] = Query(None, description="降采样桶秒数, 缺省自动"),
    max_points: int = Query(500, le=5000),
):
    """
    聚合 + 降采样查询。
    实现原理:
      - 将时间轴切分为固定 bucket 秒的桶
      - SQL: FLOOR(UNIX_TIMESTAMP(ts)/bucket) 分组, 组内计算 min/max/avg/sum
      - 大跨度(>7天)自动路由到 metric_data_hourly 预聚合表, 避免扫描原始分区
    """
    names = [m.strip() for m in metrics.split(",") if m.strip()]
    if not names:
        return {"series": {}}
    bucket_sec = bucket or _auto_bucket_seconds(start, end, max_points)
    # 原始表聚合表达式
    raw_agg = {"min": "MIN(value)", "max": "MAX(value)",
               "avg": "AVG(value)", "sum": "SUM(value)"}[agg]
    # 小时预聚合表聚合表达式 (avg 需按点数加权)
    hourly_agg = {"min": "MIN(min_value)", "max": "MAX(max_value)",
                  "avg": "SUM(sum_value)/SUM(point_count)",
                  "sum": "SUM(sum_value)"}[agg]
    start_dt = datetime.fromtimestamp(start)
    end_dt = datetime.fromtimestamp(end)
    use_hourly = (end - start) > 7 * 86400 and bucket_sec >= 3600

    result: dict[str, list] = {}
    t0 = time.perf_counter()
    with pool.acquire() as conn, conn.cursor() as cur:
        id_map = _resolve_metric_ids(cur, names, instance)
        for name, mids in id_map.items():
            if not mids:
                continue
            # 单实例用 = 走更精确索引; 跨实例用 IN 聚合
            id_cond = "metric_id=%s" if len(mids) == 1 else f"metric_id IN ({','.join(['%s'] * len(mids))})"
            if use_hourly:
                # 命中预聚合表: 对小时桶再次按 bucket_sec 分桶降采样
                sql = (
                    f"SELECT MIN(bucket_ts) AS bucket, {hourly_agg} AS v "
                    "FROM metric_data_hourly "
                    f"WHERE {id_cond} AND bucket_ts >= %s AND bucket_ts < %s "
                    "GROUP BY FLOOR(UNIX_TIMESTAMP(bucket_ts)/%s) ORDER BY bucket"
                )
            else:
                sql = (
                    f"SELECT MIN(ts) AS bucket, {raw_agg} AS v "
                    "FROM metric_data "
                    f"WHERE {id_cond} AND ts >= %s AND ts < %s "
                    "GROUP BY FLOOR(UNIX_TIMESTAMP(ts)/%s) ORDER BY bucket"
                )
            cur.execute(sql, (*mids, start_dt, end_dt, bucket_sec))
            result[name] = [
                {"ts": int(row["bucket"].timestamp()), "value": round(float(row["v"]), 4) if row["v"] is not None else None}
                for row in cur.fetchall()
            ]
    elapsed = (time.perf_counter() - t0) * 1000
    return {
        "bucket_seconds": bucket_sec,
        "source": "hourly" if use_hourly else "raw",
        "elapsed_ms": round(elapsed, 2),
        "series": result,
    }


# ---------------------------------------------------------------
# API: 最新值 (实时曲线轮询)
# ---------------------------------------------------------------
@app.get("/api/latest")
def latest_points(
    metrics: str = Query(...),
    instance: str = Query(""),
    window: int = Query(300, description="最近窗口秒数"),
):
    """返回最近 window 秒内的原始点, 用于实时曲线增量刷新。"""
    names = [m.strip() for m in metrics.split(",") if m.strip()]
    since = datetime.fromtimestamp(time.time() - window)
    result: dict[str, list] = {}
    with pool.acquire() as conn, conn.cursor() as cur:
        id_map = _resolve_metric_ids(cur, names, instance)
        for name, mids in id_map.items():
            if not mids:
                continue
            id_cond = "metric_id=%s" if len(mids) == 1 else f"metric_id IN ({','.join(['%s'] * len(mids))})"
            cur.execute(
                f"SELECT UNIX_TIMESTAMP(ts)*1000 AS ts, value FROM metric_data "
                f"WHERE {id_cond} AND ts >= %s ORDER BY ts",
                (*mids, since),
            )
            result[name] = [{"ts": int(r["ts"]), "value": r["value"]} for r in cur.fetchall()]
    return {"series": result}


# ---------------------------------------------------------------
# API: 异常点检测 (滑动窗口 Z-Score)
# ---------------------------------------------------------------
@app.get("/api/anomalies")
def detect_anomalies(
    metric: str = Query(...),
    instance: str = Query(""),
    start: float = Query(...),
    end: float = Query(...),
    window: int = Query(20, description="滑动窗口大小(点数)"),
    threshold: float = Query(3.0, description="Z-Score阈值"),
):
    """
    异常点标注: 对每个原始点, 用前 window 个点计算均值/标准差,
    |z| > threshold 判定为异常。返回异常点列表供前端高亮。
    """
    start_dt = datetime.fromtimestamp(start)
    end_dt = datetime.fromtimestamp(end)
    with pool.acquire() as conn, conn.cursor() as cur:
        id_map = _resolve_metric_ids(cur, [metric], instance)
        mids = id_map.get(metric, [])
        if not mids:
            return {"anomalies": []}
        id_cond = "metric_id=%s" if len(mids) == 1 else f"metric_id IN ({','.join(['%s'] * len(mids))})"
        cur.execute(
            f"SELECT UNIX_TIMESTAMP(ts)*1000 AS ts, value FROM metric_data "
            f"WHERE {id_cond} AND ts >= %s AND ts < %s ORDER BY ts",
            (*mids, start_dt, end_dt),
        )
        rows = cur.fetchall()

    anomalies = []
    values = [r["value"] for r in rows]
    for i, r in enumerate(rows):
        if i < window:
            continue
        w = values[i - window:i]
        mean = sum(w) / window
        var = sum((x - mean) ** 2 for x in w) / window
        std = math.sqrt(var) if var > 0 else 0
        if std > 0:
            z = abs(r["value"] - mean) / std
            if z > threshold:
                anomalies.append({"ts": int(r["ts"]), "value": r["value"], "zscore": round(z, 2)})
    return {"anomalies": anomalies}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
