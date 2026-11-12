# Scan Time Normalizer — 航空货运扫描时间归一化 / 运单时间线 API

服务包含两部分能力：

1. **扫描时间归一化**（`POST /normalize`）：将承运商导出的本地扫描时间按 IANA 时区规则归一化为 UTC；
2. **运单时间线整理**（`POST /timeline/jobs`）：把乱序导出的 gzip NDJSON 扫描流，整理成
   按运单聚合、按时间排序的逐运单时间线（gzip NDJSON），全程流式、内存有界、作业可恢复。

## 一、扫描时间归一化

将承运商导出的**本地扫描时间**按 IANA 时区规则归一化为 **UTC**，并在夏令时回拨（本地时间重复）
/ 拨快（本地时间不存在）场景下保持转运顺序语义正确：

- 每条扫描记录的本地时间对应 **0、1 或 2 个合法 UTC 候选**；
- 航段/转运衔接约束两条记录之间的 UTC 耗时落在闭区间 `[min_seconds, max_seconds]`，
  衔接可共用记录，且**全部衔接必须同时满足**；
- 服务对每条记录返回**能出现在至少一套全局可行解释中的全部候选**：
  - 仅剩一个时标 → `determined`（确定），直接给出 `utc`；
  - 仍有多个 → `needs_review`（待核对），返回全部候选，**不擅自选取**；
- 任一记录无合法候选，或衔接整体无解 → **HTTP 409** 并明确原因，**不返回部分转换**。

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

服务启动后：

- 交互式 API 文档（Swagger UI）：<http://localhost:8000/docs>
  —— 归一化接口内置三个可一键试用的示例（夏令时回拨消歧 / 不存在本地时间 409 / 衔接冲突 409）；
  在 `POST /timeline/jobs` 处可直接选择本地 gzip 文件上传，并通过作业查询与下载接口查看时间线；
- 健康检查：`GET http://localhost:8000/healthz`；
- 归一化接口：`POST http://localhost:8000/normalize`；
- 时间线作业：`POST http://localhost:8000/timeline/jobs`。

## 接口说明

### `POST /normalize`

**请求体**

| 字段 | 说明 |
| --- | --- |
| `records` | 1–200 条扫描记录：`id`（唯一）、`local_time`（精确到秒的朴素本地时间，不得带偏移）、`timezone`（IANA 时区名） |
| `links` | 0–400 条衔接：`from_id` / `to_id`（须为已声明记录）、`min_seconds` / `max_seconds`（非负整数秒，且 min ≤ max） |

**200 响应**：`records[*].status ∈ {determined, needs_review}`；`determined` 时 `utc` 为唯一可行时标，
`needs_review` 时 `utc = null` 且 `candidates` 列出全部仍可行的 UTC 候选（升序）。

**409 响应**（不返回任何部分转换）：

- `no_legal_candidate` — 本地时间在时区规则下不存在（落入拨快跳空），`invalid_records` 列出涉事记录；
- `no_global_solution` — 衔接互相矛盾、整体无解，`conflicting_links` 给出**不可约冲突子集**
  （再删掉任意一条衔接即可恢复有解），`involved_records` 列出涉事记录。

**422 响应**：请求体不合法（未知时区、重复 ID、引用未声明记录、min > max、超过 200/400 上限等）。

### 演示：夏令时回拨消歧

2024-11-03 美国冬令时回拨，纽约 `01:30` 出现两次（05:30Z / 06:30Z）：

```bash
curl -s -X POST http://localhost:8000/normalize \
  -H 'Content-Type: application/json' \
  -d '{
    "records": [
      {"id": "ARR_JFK", "local_time": "2024-11-03T01:30:00", "timezone": "America/New_York"},
      {"id": "DEP_JFK", "local_time": "2024-11-03T02:45:00", "timezone": "America/New_York"},
      {"id": "ARR_ORD", "local_time": "2024-11-03T04:10:00", "timezone": "America/Chicago"},
      {"id": "DEP_LAX", "local_time": "2024-11-03T03:20:00", "timezone": "America/Los_Angeles"},
      {"id": "XFR_ORD", "local_time": "2024-11-03T01:30:00", "timezone": "America/Chicago"}
    ],
    "links": [
      {"from_id": "ARR_JFK", "to_id": "DEP_JFK", "min_seconds": 3600, "max_seconds": 7200},
      {"from_id": "DEP_JFK", "to_id": "ARR_ORD", "min_seconds": 7200, "max_seconds": 10800},
      {"from_id": "ARR_ORD", "to_id": "DEP_LAX", "min_seconds": 1800, "max_seconds": 7200},
      {"from_id": "ARR_JFK", "to_id": "XFR_ORD", "min_seconds": 0, "max_seconds": 86400}
    ]
  }'
```

实际返回（节选）：

```json
{
  "status": "ok",
  "summary": {"total": 5, "determined": 4, "needs_review": 1},
  "records": [
    {"id": "ARR_JFK", "status": "determined", "utc": "2024-11-03T06:30:00Z", "candidates": ["2024-11-03T06:30:00Z"], "...": "..."},
    {"id": "XFR_ORD", "status": "needs_review", "utc": null, "candidates": ["2024-11-03T06:30:00Z", "2024-11-03T07:30:00Z"], "...": "..."}
  ]
}
```

`ARR_JFK` 的两个回拨候选中，`05:30Z` 会使到 `DEP_JFK`（07:45Z）的地面耗时达 2h15m、
超出 1–2h 窗口，被全局约束排除，故唯一确定为 `06:30Z`；`XFR_ORD` 的两个候选都满足全部
衔接，保持 `needs_review`，交由人工核对。

## 二、运单时间线整理（文件整理 API）

接收承运商乱序导出的 **gzip 压缩 UTF-8 NDJSON**，后台整理为按运单聚合的逐运单时间线。

### 输入约定

- 上传：`multipart/form-data`，字段名 `file`，gzip 压缩的 UTF-8 NDJSON；
- 每行一个 JSON 对象，**仅含**四个字段：
  - `awb`、`scan_id`、`airport`：1–64 字符字符串；
  - `utc`：已确认的 UTC 整秒时刻（整数 epoch 秒，或带 `Z`/`+00:00` 的 ISO-8601 字符串，
    不得带亚秒、不得为无时区的本地时间）；
- 同一文件内 `scan_id` 全局唯一语义：
  - **四字段完全相同**的重复行只保留一次（计入 `duplicate_events`）；
  - 同 `scan_id` 但内容不同 → **整次作业失败**（`duplicate_conflict`），不产生任何结果。

### 输出约定

后台生成 gzip NDJSON：

- 每行一个运单对象，按 `awb` 字典序排列：`{"awb": ..., "events": [...]}`；
- `events` 按 `(utc, scan_id)` 升序，保留全部原始字段（整数 utc 仍是整数、字符串 utc 仍是字符串）；
- 结果先写 `timeline.ndjson.gz.tmp`，全部成功后**原子重命名**为 `timeline.ndjson.gz`——
  即使在最后一行才发现格式错误、gzip 损坏或写盘失败，也绝不会留下可下载的部分结果。

### 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/timeline/jobs` | 上传 gzip NDJSON（Swagger UI 中直接选文件），返回 `202` 与作业状态 |
| GET | `/timeline/jobs` | 列出全部作业（状态、错误、统计） |
| GET | `/timeline/jobs/{job_id}` | 查询单个作业的状态和错误 |
| GET | `/timeline/jobs/{job_id}/download` | 下载完成的 gzip 时间线；未成功完成返回 **409**，未知作业 404 |

作业状态：`pending → running → done | failed`。`failed` 的 `error.code` 取值：
`invalid_input`（NDJSON/字段/UTC 不合法）、`gzip_corrupt`、`duplicate_conflict`、
`write_failed`、`interrupted`（重启中断）、`internal_error`。

```bash
# 1) 上传
curl -X POST http://localhost:8000/timeline/jobs -F 'file=@scans.ndjson.gz;type=application/gzip'
# 2) 轮询
curl http://localhost:8000/timeline/jobs/<job_id>
# 3) 下载
curl -OJ http://localhost:8000/timeline/jobs/<job_id>/download
```

### 内存有界的流式设计（不随文件大小增长）

接收、解压、查重、排序、写出五个环节都不会把全文件、全部 ID 或单个运单的全部事件载入内存：

1. **接收**：上传按 1 MiB 分块直接落盘；
2. **解压/解析**：`gzip` 逐行读取，单行硬上限 1 MiB，逐行校验；
3. **查重**：每个作业一个独立 SQLite 工作库，`scan_id` 建唯一索引，页缓存上限 20 MiB，
   B 树与临时排序落盘——全部 ID 只存在磁盘上，不在 Python 内存里；
4. **排序**：复合索引 `(awb, utc_epoch, scan_id)` + 惰性游标，逐行产出，
   不物化任何运单的事件数组；
5. **写出**：固定 64 KiB 缓冲逐行写 gzip，`fsync` 后原子重命名发布。

30 万事件（含单个 20 万事件的运单）实测峰值 RSS 约 28 MiB，与 3 万事件基本持平。

### 重启恢复与文件保留

作业注册表保存在数据卷的 SQLite 中：

- 服务重启时，所有 `pending`/`running` 作业标记为 `failed`（`error.code = interrupted`）；
- `done` 作业状态与结果文件保留，仍可下载；`failed` 作业保留注册记录与原始上传以便排查。

数据目录由环境变量 `TIMELINE_DATA_DIR` 配置（容器内默认 `/data`，Compose 中为命名卷）。

## 三、归一化实现原理

1. **候选生成**（`app/solver.py: utc_candidates`）：对本地时间分别以 `fold=0/1` 附着时区后转 UTC，
   再回读校验——回拨重复得到 2 个候选，拨快跳空得到 0 个，常规时间 1 个。
2. **全局可行性 = 2-SAT**：每条含 2 个候选的记录对应一个布尔变量；每条衔接把"越界的候选组合"
   翻译成子句（禁止该组合）。2-SAT 可用蕴含图 + 强连通分量在线性时间内判定。
3. **逐候选过滤**：对每个候选施加单元子句假设后重判可满足性，保留下来的即为
   "能出现在至少一套全局可行解释中"的候选——精确求解，非启发式。
4. **冲突定位**：整体无解时，对衔接做贪心删除得到不可约冲突子集，作为 409 的明确原因。

规模上限（200 记录 / 400 衔接）下，最多约 400 次小规模 2-SAT 判定，响应为毫秒级。

## 本地开发与测试

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt pytest httpx
uvicorn app.main:app --reload          # 本地起服务
python -m pytest tests/ -q             # 33 个用例：归一化 19 + 时间线作业 14
```

## 目录结构

```
app/
  main.py      # FastAPI 入口、lifespan、/normalize 路由、Swagger
  schemas.py   # 归一化请求/响应模型与输入校验
  solver.py    # UTC 候选生成 + 2-SAT 全局可行性 + 不可约冲突子集
  timeline/
    config.py     # 数据目录/缓存/分块等运行配置（环境变量）
    processor.py  # 流式管线：逐行解压校验 → SQLite 查重排序 → 原子发布 gzip
    store.py      # 作业注册表（SQLite）、线程池、重启恢复
    schemas.py    # 作业响应模型
    api.py        # /timeline/jobs 上传/查询/下载路由
tests/test_api.py        # 归一化接口测试（19）
tests/test_timeline.py   # 时间线作业端到端测试（14）
Dockerfile / docker-compose.yml / requirements.txt
```
