<template>
  <div>
    <div class="header">
      <h1>时序数据监控平台 · Time Series DB</h1>
      <div class="stats">
        <div class="stat"><div class="num">{{ metricsList.length }}</div><div class="label">指标</div></div>
        <div class="stat"><div class="num">{{ totalPoints.toLocaleString() }}</div><div class="label">数据点</div></div>
        <div class="stat">
          <div class="num" :style="{ color: live ? '#66bb6a' : '#7a869e' }">●</div>
          <div class="label">{{ live ? '实时采集中' : '已暂停' }}</div>
        </div>
      </div>
    </div>

    <div class="toolbar">
      <div class="group">
        <label>实例</label>
        <select v-model="instance" @change="onQuery">
          <option v-for="i in instances" :key="i" :value="i">{{ i || '全部' }}</option>
        </select>
      </div>
      <div class="group">
        <label>指标</label>
        <div class="metric-chips">
          <span
            v-for="m in metricNames"
            :key="m"
            class="chip"
            :class="{ on: selected.includes(m) }"
            @click="toggleMetric(m)"
          >{{ m }}</span>
        </div>
      </div>
    </div>

    <div class="toolbar">
      <div class="group">
        <label>时间范围</label>
        <button v-for="r in ranges" :key="r.sec" :class="{ active: rangeSec === r.sec }" @click="setRange(r.sec)">
          {{ r.label }}
        </button>
      </div>
      <div class="group">
        <label>聚合</label>
        <button v-for="a in ['min','max','avg','sum']" :key="a" :class="{ active: agg === a }" @click="setAgg(a)">
          {{ a.toUpperCase() }}
        </button>
      </div>
      <div class="group">
        <button :class="{ active: live }" @click="toggleLive">{{ live ? '暂停实时' : '开启实时' }}</button>
        <button class="primary" @click="onQuery">刷新查询</button>
      </div>
    </div>

    <div class="chart-card">
      <h3>多指标对比 · 降采样曲线（聚合: {{ agg.toUpperCase() }} / 桶: {{ queryMeta.bucket }}s ·
        数据源: {{ queryMeta.source === 'hourly' ? '小时预聚合表' : '原始分区表' }} ·
        耗时: {{ queryMeta.elapsed }}ms · 异常点: {{ anomalyCount }}）</h3>
      <div ref="historyChart" class="chart chart-tall"></div>
    </div>

    <div class="chart-card">
      <h3>实时曲线（最近 {{ liveWindow }} 秒原始点）· 异常点以红色标注</h3>
      <div ref="liveChart" class="chart"></div>
      <div class="meta">
        <span><span class="legend-dot" style="background:#ff5252"></span>异常点 (滑动窗口 Z-Score &gt; {{ anomalyThreshold }})</span>
        <span>异常总数: <span class="hl">{{ anomalyCount }}</span></span>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted, onBeforeUnmount, nextTick } from 'vue'
import * as echarts from 'echarts'

const API = ''

const metricsList = ref([])
const totalPoints = ref(0)
const instances = ref([''])
const instance = ref('')
const metricNames = ref([])
const selected = ref(['cpu.usage', 'mem.usage'])
const ranges = [
  { sec: 900, label: '15分钟' },
  { sec: 3600, label: '1小时' },
  { sec: 21600, label: '6小时' },
  { sec: 86400, label: '24小时' },
  { sec: 172800, label: '2天' },
]
const rangeSec = ref(21600)
const agg = ref('avg')
const live = ref(true)
const liveWindow = 300
const anomalyThreshold = 3.0
const anomalyCount = ref(0)

const queryMeta = reactive({ bucket: '-', source: 'raw', elapsed: '-' })

const historyChart = ref(null)
const liveChart = ref(null)
let histInst = null
let liveInst = null
let liveTimer = null
let liveAnomalyTimer = null

const COLORS = ['#4fc3f7', '#ffb74d', '#81c784', '#ba68c8', '#f06292', '#4dd0e1']

async function fetchJson(url) {
  const r = await fetch(API + url)
  return r.json()
}

async function loadMetrics() {
  const rows = await fetchJson('/api/metrics')
  metricsList.value = rows
  totalPoints.value = rows.reduce((s, r) => s + Number(r.points || 0), 0)
  const instSet = [...new Set(rows.map(r => r.instance))]
  instances.value = ['', ...instSet]
  const names = [...new Set(rows.map(r => r.name))]
  metricNames.value = names
  if (!selected.value.some(s => names.includes(s)) && names.length) {
    selected.value = names.slice(0, 2)
  }
}

function toggleMetric(m) {
  const i = selected.value.indexOf(m)
  if (i >= 0) selected.value.splice(i, 1)
  else selected.value.push(m)
  onQuery()
}
function setRange(s) { rangeSec.value = s; onQuery() }
function setAgg(a) { agg.value = a; onQuery() }
function toggleLive() { live.value = !live.value; scheduleLive() }

// ---------- 历史查询: 多指标对比 + 降采样 ----------
async function onQuery() {
  if (!selected.value.length) { histInst.setOption({ series: [] }); return }
  const end = Math.floor(Date.now() / 1000)
  const start = end - rangeSec.value
  const q = new URLSearchParams({
    metrics: selected.value.join(','),
    instance: instance.value,
    start: String(start), end: String(end),
    agg: agg.value,
  })
  const data = await fetchJson('/api/query?' + q.toString())
  queryMeta.bucket = data.bucket_seconds
  queryMeta.source = data.source
  queryMeta.elapsed = data.elapsed_ms

  const series = Object.entries(data.series || {}).map(([name, pts], idx) => ({
    name,
    type: 'line',
    smooth: true,
    showSymbol: false,
    lineStyle: { width: 2 },
    itemStyle: { color: COLORS[idx % COLORS.length] },
    data: pts.map(p => [p.ts * 1000, p.value]),
    connectNulls: true,
  }))

  // 对第一个选中指标做异常点检测, 在历史曲线上以红色散点标注
  const anomalies = await fetchAnomalies(selected.value[0], start, end)
  anomalyCount.value = anomalies.length
  if (anomalies.length) {
    series.push({
      name: '异常点',
      type: 'scatter',
      symbolSize: 11,
      itemStyle: { color: '#ff5252', borderColor: '#fff', borderWidth: 1 },
      data: anomalies.map(a => [a.ts, a.value]),
      z: 10,
    })
  }

  histInst.setOption(buildBaseOption(false, series), true)
}

async function fetchAnomalies(metric, start, end) {
  if (!metric) return []
  const q = new URLSearchParams({
    metric, instance: instance.value,
    start: String(start), end: String(end),
    threshold: String(anomalyThreshold),
  })
  const data = await fetchJson('/api/anomalies?' + q.toString())
  return data.anomalies || []
}

// ---------- 实时曲线 + 异常点标注 ----------
async function refreshLive() {
  if (!selected.value.length) return
  const q = new URLSearchParams({
    metrics: selected.value.join(','),
    instance: instance.value,
    window: String(liveWindow),
  })
  const data = await fetchJson('/api/latest?' + q.toString())

  const series = Object.entries(data.series || {}).map(([name, pts], idx) => ({
    name,
    type: 'line',
    smooth: true,
    showSymbol: false,
    lineStyle: { width: 2 },
    itemStyle: { color: COLORS[idx % COLORS.length] },
    data: pts.map(p => [p.ts, p.value]),
    connectNulls: true,
  }))
  liveInst.setOption(buildBaseOption(true, series), { replaceMerge: ['series'] })
}

async function refreshAnomalies() {
  // 对第一个选中指标做异常点标注
  const metric = selected.value[0]
  if (!metric) return
  const end = Math.floor(Date.now() / 1000)
  const start = end - liveWindow
  const q = new URLSearchParams({ metric, instance: instance.value, start: String(start), end: String(end), threshold: String(anomalyThreshold) })
  const data = await fetchJson('/api/anomalies?' + q.toString())
  const anomalies = data.anomalies || []
  anomalyCount.value = anomalies.length
  const scatter = {
    name: '异常点',
    type: 'scatter',
    symbolSize: 12,
    itemStyle: { color: '#ff5252', borderColor: '#fff', borderWidth: 1 },
    data: anomalies.map(a => [a.ts, a.value]),
    z: 10,
    tooltip: {
      formatter: p => `异常点<br/>值: ${p.value[1].toFixed(2)}<br/>${new Date(p.value[0]).toLocaleTimeString()}`,
    },
  }
  // 追加异常点散点序列(不清空已有曲线)
  const opt = liveInst.getOption()
  liveInst.setOption({ series: [...opt.series.filter(s => s.name !== '异常点'), scatter] })
}

function buildBaseOption(isLive, series) {
  return {
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'axis',
      backgroundColor: '#1e2940',
      borderColor: '#31405e',
      textStyle: { color: '#d7dde8' },
    },
    legend: {
      data: series.map(s => s.name),
      textStyle: { color: '#8b96ad' },
      top: 0,
    },
    grid: { left: 56, right: 24, top: 40, bottom: 56 },
    xAxis: {
      type: 'time',
      axisLine: { lineStyle: { color: '#31405e' } },
      axisLabel: { color: '#7a869e' },
      splitLine: { show: false },
    },
    yAxis: {
      type: 'value',
      scale: true,
      axisLine: { lineStyle: { color: '#31405e' } },
      axisLabel: { color: '#7a869e' },
      splitLine: { lineStyle: { color: '#1d273b' } },
    },
    dataZoom: isLive
      ? [{ type: 'inside' }, { type: 'slider', height: 18, bottom: 12, borderColor: '#31405e', fillerColor: 'rgba(79,195,247,0.15)' }]
      : [{ type: 'inside' }, { type: 'slider', height: 18, bottom: 12, borderColor: '#31405e', fillerColor: 'rgba(79,195,247,0.15)' }],
    series,
  }
}

function scheduleLive() {
  clearInterval(liveTimer)
  clearInterval(liveAnomalyTimer)
  if (live.value) {
    liveTimer = setInterval(refreshLive, 2000)
    liveAnomalyTimer = setInterval(refreshAnomalies, 5000)
    refreshLive()
    refreshAnomalies()
  }
}

onMounted(async () => {
  await nextTick()
  histInst = echarts.init(historyChart.value, 'dark')
  liveInst = echarts.init(liveChart.value, 'dark')
  window.addEventListener('resize', () => { histInst.resize(); liveInst.resize() })
  await loadMetrics()
  await onQuery()
  scheduleLive()
  // 指标列表每 10 秒刷新一次统计
  setInterval(loadMetrics, 10000)
})

onBeforeUnmount(() => {
  clearInterval(liveTimer)
  clearInterval(liveAnomalyTimer)
})
</script>
