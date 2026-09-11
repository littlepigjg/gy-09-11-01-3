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


def load_writable_metrics():
    """从元数据服务获取仍可写入的指标(active/silent), 跳过 archived。

    模拟器不缓存 HTTP 失败时的状态判断: 拉取失败则保留完整指标集合,
    让后端写入接口继续作为最终裁决, 避免短暂接口抖动导致模拟数据断流。
    """
    base = API.rsplit("/", 1)[0]
    try:
        r = requests.get(base + "/metrics", timeout=10)
        r.raise_for_status()
        archived = {
            (m.get("name"), m.get("instance", ""))
            for m in r.json()
            if m.get("status") == "archived"
        }
        writable = [m for m in METRICS if (m[0], m[1]) not in archived]
        print(f"可写入指标 {len(writable)}/{len(METRICS)} (静默指标仍正常采集)")
        return writable
    except Exception as e:
        print(f"获取指标状态失败, 暂按全部指标生成: {e}")
        return list(METRICS)


def apply_rejected(payload, writable_metrics):
    """根据混合批次响应移除归档指标, 返回本批次新发现的归档指标。

    归档点由后端按语义拒绝, 不能重试或补写到其他指标。模拟器立即调整
    后续候选集合, 使下一批仍由 active/silent 指标填满, 保持固定吞吐节奏。
    """
    rejected = payload.get("rejected") or []
    if not rejected:
        return set()

    current = {(name, instance): (name, instance, base, amp)
               for name, instance, base, amp in writable_metrics}
    newly_archived = set()
    for item in rejected:
        key = (item.get("metric"), item.get("instance", ""))
        if key in current:
            current.pop(key)
            newly_archived.add(key)

    writable_metrics[:] = list(current.values())
    return newly_archived


def gen_batch(batch_size: int = 200, writable_metrics=None):
    """生成一批数据点, 含正弦周期 + 噪声 + 偶发尖刺(异常点)。"""
    candidates = writable_metrics if writable_metrics is not None else METRICS
    if not candidates:
        return []

    now = time.time()
    points = []
    for _ in range(batch_size):
        name, inst, base, amp = random.choice(candidates)
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


def backfill(hours: int = 48, writable_metrics=None):
    """回填历史数据, 用于验证分区、降采样与聚合查询。"""
    writable_metrics = writable_metrics if writable_metrics is not None else list(METRICS)
    if not writable_metrics:
        print("没有可写入指标, 跳过历史回填")
        return

    print(f"回填最近 {hours} 小时历史数据...")
    step = 60  # 每分钟一个点
    total = hours * 3600 // step
    batch = []
    inserted = 0

    def flush():
        nonlocal batch, inserted
        if not batch:
            return
        r = requests.post(API, json={"points": batch}, timeout=30)
        r.raise_for_status()
        payload = r.json()
        inserted += int(payload.get("inserted") or 0)
        newly = apply_rejected(payload, writable_metrics)
        for name, instance in newly:
            print(f"  指标已归档, 后续回填跳过: {name}/{instance or '-'}")
        print(f"  本批接受 {payload.get('inserted', 0)} 点, 拒绝 {len(payload.get('rejected') or [])} 点")
        batch = []

    for i in range(total):
        if not writable_metrics:
            print("全部指标均已归档, 停止回填")
            break
        ts = time.time() - (total - i) * step
        # 每轮复制列表, 避免遍历过程中归档移除元素造成跳变。
        for name, inst, base, amp in list(writable_metrics):
            v = base + amp * math.sin(ts / 3600 * 2 * math.pi) + random.gauss(0, amp * 0.1)
            if random.random() < 0.005:
                v += base * random.choice([1.5, -0.8])
            batch.append({"metric": name, "instance": inst, "ts": ts, "value": round(max(v, 0.0), 3)})
        if len(batch) >= 2000:
            flush()
            print(f"  已处理 {i + 1}/{total} 个时间点, 累计接受 {inserted} 点")
    flush()
    print(f"回填完成, 累计接受 {inserted} 点")


def live(interval: float = 2.0, writable_metrics=None):
    """实时模式: 持续写入。"""
    writable_metrics = writable_metrics if writable_metrics is not None else load_writable_metrics()
    accepted_total = 0
    rejected_total = 0
    ticks = 0
    print("实时写入中 (Ctrl+C 停止)...")

    while True:
        started = time.perf_counter()
        try:
            # 每约 60 秒刷新一次状态, 用于感知 archived -> active 后的恢复写入。
            ticks += 1
            if ticks % 30 == 1 and ticks > 1:
                writable_metrics[:] = load_writable_metrics()

            if not writable_metrics:
                print("全部指标已归档, 等待状态恢复...")
                writable_metrics[:] = load_writable_metrics()
            else:
                batch = gen_batch(writable_metrics=writable_metrics)
                r = requests.post(API, json={"points": batch}, timeout=10)
                r.raise_for_status()
                payload = r.json()
                accepted = int(payload.get("inserted") or 0)
                rejected = payload.get("rejected") or []
                accepted_total += accepted
                rejected_total += len(rejected)

                newly = apply_rejected(payload, writable_metrics)
                for name, instance in newly:
                    print(f"指标已归档, 后续批次不再生成: {name}/{instance or '-'}")
                print(
                    f"本批接受 {accepted} 点, 拒绝 {len(rejected)} 点; "
                    f"累计接受 {accepted_total}, 累计归档拒绝 {rejected_total}, "
                    f"活跃候选 {len(writable_metrics)}/{len(METRICS)}"
                )
        except Exception as e:
            print(f"写入失败: {e}")

        # 扣除请求耗时, 保持稳定的提交节拍; 请求超时时立即进入下一轮。
        elapsed = time.perf_counter() - started
        time.sleep(max(0.0, interval - elapsed))


def auto(hours: int = 48):
    """容器编排默认入口: 数据库为空时先回填历史数据, 再进入实时写入。
    已存在数据则跳过回填, 避免容器重启时重复灌入。"""
    writable_metrics = load_writable_metrics()
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
        backfill(hours, writable_metrics)
    live(writable_metrics=writable_metrics)


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "backfill":
        backfill(int(sys.argv[2]) if len(sys.argv) > 2 else 48, load_writable_metrics())
    elif len(sys.argv) > 1 and sys.argv[1] == "auto":
        auto(int(sys.argv[2]) if len(sys.argv) > 2 else 48)
    else:
        live()
