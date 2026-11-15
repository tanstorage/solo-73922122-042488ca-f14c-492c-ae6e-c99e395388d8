# 门店日结模型试跑平台

基于 **FastAPI + PostgreSQL** 的数据模型试跑环境。开发者修改门店日结清洗 SQL 后，
可以在网页上发起试跑、定位失败模型、修正 SQL 后只重跑失败及受阻节点，
并且**整次试跑始终绑定启动那一刻已提交的数据时点，绝不混用不同时点的数据**。
历史成功批次占用的快照空间可以在线回收，同时完整保留批次、节点与事件供复盘。

## 一键启动

```bash
docker compose up --build
```

- 网页（唯一浏览器入口，经 nginx 轮询到两个 API 实例）：<http://localhost:8000/>
- API 文档（Swagger）：<http://localhost:8000/docs>
- PostgreSQL：本机 `localhost:55432`，库名 `tryout`，用户/密码 `postgres/postgres`

首次启动会自动建表、创建低权限角色 `model_runner`、写入 `sales`/`refunds`
预置数据与两个示例模型（`daily_sales_clean` → `daily_net_sales`）。

### 多实例拓扑

`docker compose` 启动 **2 个 `api` 副本 + 1 个 `web`(nginx) 负载均衡 + 1 个
PostgreSQL**。浏览器只访问 nginx 的 8000 端口，请求被轮询到任一 API 实例；
两个实例共享同一个数据库，所有并发安全保证都落在数据库锁与事务上
（不依赖进程内状态），因此同一批次在两个实例同时点“回收”也只会真正清理一次。

## 功能与操作

浏览器三个页签：

1. **源数据**：预览 `sales`、`refunds`；上传 CSV **整表覆盖**（带表头/类型校验，
   失败回滚不污染原数据）。表头要求：
   - `sales`: `id,store_id,biz_date,amount`
   - `refunds`: `id,store_id,biz_date,refund_amount`
2. **模型定义**：最多 4 个模型；只允许单条 `SELECT` / `WITH ... SELECT`；
   依赖必须无环（Kahn 拓扑校验）。试跑进行中可改 SQL（供失败重试），
   但模型名/依赖等结构变更需等批次结束。
3. **试跑批次**：启动试跑、查看每个节点状态与失败原因、查看结果表、
   对失败批次点「重试」。

## 核心设计：如何保证数据时点一致

每次启动试跑时，系统在一个**独立、之后不再变化的 schema** `run_snap_<run_id>` 内：

1. 用 `CTAS + INSERT` 把当时的 `sales`/`refunds` 完整复制为快照；
2. 把每个模型当时的 SQL、依赖关系冻结进 `run_nodes`；
3. 节点执行时 `SET search_path = run_snap_<id>, public`，模型里的裸表名
   （源表名、上游模型名）全部解析到该快照，物理上读不到后来的新数据。

因此即使：

- 试跑中途有人上传 CSV 覆盖了源表；
- 服务重启；
- 多次重试；

该批次读到的源数据、已成功上游的结果都保持启动时点不变。重试只把
`failed`/`blocked` 节点刷新为模型当前 SQL 后重新执行，`succeeded` 节点
（含其结果表）原样复用。

## 快照空间回收（保留复盘痕迹）

试跑反复迭代后，旧批次在各自 `run_snap_<id>` schema 里冻结的源表与结果表
会持续占用空间。批次列表中对**非最新的成功批次**提供「回收快照空间」按钮：

- **只删数据、不删记录**：仅删除该批次专属 schema 内的冻结源表与各节点
  结果表，并移除已清空的 schema；批次、节点、事件全部保留，列表与详情页
  会标注紫色「结果已回收」徽章与回收时间，回收动作本身也写入事件流。
- **准入条件**：批次状态必须是 `succeeded`，且不是最新批次（最新批次保留
  完整结果供对照）。失败批次、运行中批次、最新批次一律拒绝（HTTP 409）。
- **回收与查询交错**：
  - 已受理、尚未结束的结果查询持有快照表的 `ACCESS SHARE` 锁。回收事务
    删除前先探测 `pg_locks`，发现表正被读取就**整体放弃并返回 503**
    （不删任何东西，可安全重试），保证在途查询一定能读完完整结果；
  - 回收提交之后发起的新查询先读到 `runs.reclaimed_at` 标记，直接得到
    **HTTP 410「结果已回收、不可读取」**的明确响应，而不是报错或空表。
- **多实例只清理一次**：回收以 `SELECT ... FOR UPDATE` 锁定批次行，把
  并发请求（含两个 API 实例上的同时点击）串行化；只有第一个请求执行删除，
  其余请求得到 409「已回收」。
- **失败/退出不产生不一致**：删表、删 schema、写回收标记、写事件在**同一个
  数据库事务**内提交，任何数据库错误或进程退出都整体回滚，绝不会出现
  “标记已打但表还在”或“表已删但未标记”。
- **不影响失败重试**：失败批次的快照不在回收范围内，仍凭原快照重试
  （见下文重试语义）；回收也只作用于目标批次自己的 schema。

> 升级已有数据卷时（`init.sql` 仅对空卷自动执行），需手动补一列：
> `ALTER TABLE public.runs ADD COLUMN IF NOT EXISTS reclaimed_at TIMESTAMPTZ;`

## 失败语义（不留半成品、不误报成功）

- 每个节点在**单事务**内完成 `CREATE TABLE 结果表 AS SELECT` + 状态更新；
  任何报错整体回滚，快照里不会留下半成品表。
- 节点失败后，其全部下游递归置为 `blocked`；互不依赖的其他分支继续执行。
- 互不依赖的节点在线程池（默认 4 并发）中并行，各自独立连接/事务。
- 只有所有节点到达终态后批次才会被置为 `succeeded`，否则为 `failed`；
  进程中途退出时数据库里不会出现「成功」标记。
- 重启时 `recover_on_startup` 把残留的 `running` 节点重置为 `pending`
  （其事务已随旧进程回滚），自动续跑；已提交的成功节点不重算。

## SQL 安全防线

模型 SQL 经过两层约束：

1. **应用层守卫**（`app/sql_guard.py`）：单语句、仅 SELECT/WITH、禁止
   INSERT/UPDATE/DELETE/DDL/事务控制等关键字、禁止 schema 限定名与 `t.*`、
   引用表必须是源表或声明的上游；
2. **数据库层降权**：执行时 `SET LOCAL ROLE model_runner`，该角色对平台表
   只读、只能在自己的快照 schema 内建结果表，无法篡改源表与平台元数据，
   并设置 `statement_timeout`（默认 30s）。

## API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET/POST | `/api/models` | 列表 / 新建模型 |
| PUT/PATCH/DELETE | `/api/models/{id}`、`/api/models/{id}/sql` | 改结构 / 仅改 SQL / 删除 |
| GET | `/api/sources`、`/api/sources/{table}` | 源表预览 |
| POST | `/api/sources/{table}/upload` | CSV 整表覆盖 |
| POST | `/api/runs` | 启动试跑（冻结快照） |
| GET | `/api/runs`、`/api/runs/{id}` | 批次列表 / 详情（节点+事件+错误） |
| POST | `/api/runs/{id}/retry` | 仅重跑失败/阻塞节点 |
| POST | `/api/runs/{id}/reclaim` | 回收非最新成功批次的专属快照（409/410/503 见下） |
| GET | `/api/runs/{id}/nodes/{nid}/result` | 节点结果数据；快照回收后返回 410 |

回收接口的状态码：`200` 回收成功；`404` 批次不存在；`409` 不是成功批次 /
是最新批次 / 已被其他实例回收（幂等冲突，清理不会重复）；`503` 快照正被
在途查询占用，本次未做任何修改，可稍后重试。

## 本地测试

测试需要一个已初始化的 PostgreSQL（执行 `db/init.sql`）：

```bash
export DATABASE_URL="postgresql://postgres@localhost:55432/tryout"
pip install -r requirements.txt pytest
pytest tests/ -q
```

覆盖：SQL 守卫与 DAG 校验、成功试跑与结果、失败阻塞与其他分支继续、
CSV 校验与回滚、改源表后重试的快照隔离、跨批次时点隔离、半成品表回滚、
低权限角色写保护、模拟崩溃的重启恢复、并行执行观测、重试边界，以及
快照回收的准入规则、在途查询交错（503/410/完整结果）、多实例并发只清理
一次、故障回滚后标记与快照一致、失败批次重试不受回收影响。

## 目录结构

```
db/init.sql            数据库初始化（表、角色、权限、种子数据与示例模型）
app/config.py          环境配置
app/db.py              连接池
app/sql_guard.py       SELECT-only 与引用合法性守卫
app/dag.py             无环校验 / 拓扑排序
app/scheduler.py       快照冻结、并行调度、失败阻塞、重试、重启恢复
app/services.py        模型 CRUD 与校验
app/runs_service.py    CSV 上传、批次/结果查询（回收后返回 410）
app/reclaim_service.py 历史成功批次的快照回收（行锁串行化、单事务一致性）
app/main.py            FastAPI 路由与生命周期
app/web/               原生 JS 单页前端
tests/                 端到端、恢复与快照回收测试
nginx.conf             浏览器入口，轮询两个 API 副本
docker-compose.yml     db + 双 api 副本 + nginx，带健康检查
```
