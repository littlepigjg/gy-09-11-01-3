"""模拟传感器/监控指标数据生成器: 持续向时序服务批量写入数据。"""
import math
import os
import random
import time

import requests

API = os.getenv("API_URL", "http://127.0.0.1:8000") + "/api/write"
METRICS = [
    ("cpu.usage", "host-1", 30.0, 20.0),      # 基线30, 振幅20
    ("cpu.usage", "host-2", 45.0, 15.0),
    ("mem.usage", "host-1", 60.0, 10.0),
    ("mem.usage", "host-2", 55.0, 12.0),
    ("disk.io", "host-1", 100.0, 60.0),
    ("net.rx_mbps", "host-1", 50.0, 30.0),
]


def gen_batch(batch_size: int = 200):
    """生成一批数据点, 含正弦周期 + 噪声 + 偶发尖刺(异常点)。"""
    now = time.time()
    points = []
    for _ in range(batch_size):
        name, inst, base, amp = random.choice(METRICS)
        # 正弦周期(1小时) + 高斯噪声
        v = base + amp * math.sin(now / 3600 * 2 * math.pi) + random.gauss(0, amp * 0.1)
        # 2% 概率注入异常尖刺
        if random.random() < 0.02:
            v += base * random.choice([1.5, -0.8])
        points.append({
            "metric": name,
            "instance": inst,
            "ts": now - random.uniform(0, 5),
            "value": round(max(v, 0.0), 3),
        })
    return points


def backfill(hours: int = 48):
    """回填历史数据, 用于验证分区、降采样与聚合查询。"""
    print(f"回填最近 {hours} 小时历史数据...")
    step = 60  # 每分钟一个点
    total = hours * 3600 // step
    batch = []
    for i in range(total):
        ts = time.time() - (total - i) * step
        for name, inst, base, amp in METRICS:
            v = base + amp * math.sin(ts / 3600 * 2 * math.pi) + random.gauss(0, amp * 0.1)
            if random.random() < 0.005:
                v += base * random.choice([1.5, -0.8])
            batch.append({"metric": name, "instance": inst, "ts": ts, "value": round(max(v, 0.0), 3)})
        if len(batch) >= 2000:
            r = requests.post(API, json={"points": batch}, timeout=30)
            print(f"  已写入 {i}/{total} 批, 响应: {r.json()}")
            batch.clear()
    if batch:
        requests.post(API, json={"points": batch}, timeout=30)
    print("回填完成")


def live(interval: float = 2.0):
    """实时模式: 持续写入。"""
    print("实时写入中 (Ctrl+C 停止)...")
    while True:
        try:
            r = requests.post(API, json={"points": gen_batch()}, timeout=10)
            print(f"写入 {r.json()}")
        except Exception as e:
            print(f"写入失败: {e}")
        time.sleep(interval)


def auto(hours: int = 48):
    """容器编排默认入口: 数据库为空时先回填历史数据, 再进入实时写入。
    已存在数据则跳过回填, 避免容器重启时重复灌入。"""
    base = API.rsplit("/", 1)[0]  # http://host:8000/api
    need_backfill = True
    try:
        r = requests.get(base + "/metrics", timeout=10)
        total = sum(int(m.get("points") or 0) for m in r.json())
        need_backfill = total == 0
        print(f"当前已有数据点 {total}, {'需要回填' if need_backfill else '跳过回填'}")
    except Exception as e:
        print(f"检查数据状态失败, 默认回填: {e}")
    if need_backfill:
        backfill(hours)
    live()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "backfill":
        backfill(int(sys.argv[2]) if len(sys.argv) > 2 else 48)
    elif len(sys.argv) > 1 and sys.argv[1] == "auto":
        auto(int(sys.argv[2]) if len(sys.argv) > 2 else 48)
    else:
        live()
