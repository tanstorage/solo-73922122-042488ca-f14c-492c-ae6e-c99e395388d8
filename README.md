# 门店日结模型试跑平台

基于 **FastAPI + PostgreSQL** 的数据模型试跑环境。开发者修改门店日结清洗 SQL 后，
可以在网页上发起试跑、定位失败模型、修正 SQL 后只重跑失败及受阻节点，
并且**整次试跑始终绑定启动那一刻已提交的数据时点，绝不混用不同时点的数据**。

## 一键启动

```bash
docker compose up --build
```

- 网页：<http://localhost:8000/>
- API 文档（Swagger）：<http://localhost:8000/docs>
- PostgreSQL：本机 `localhost:55432`，库名 `tryout`，用户/密码 `postgres/postgres`

首次启动会自动建表、创建低权限角色 `model_runner`、写入 `sales`/`refunds`
预置数据与两个示例模型（`daily_sales_clean` → `daily_net_sales`）。

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
| GET | `/api/runs/{id}/nodes/{nid}/result` | 节点结果数据 |

## 本地测试

测试需要一个已初始化的 PostgreSQL（执行 `db/init.sql`）：

```bash
export DATABASE_URL="postgresql://postgres@localhost:55432/tryout"
pip install -r requirements.txt pytest
pytest tests/ -q
```

覆盖：SQL 守卫与 DAG 校验、成功试跑与结果、失败阻塞与其他分支继续、
CSV 校验与回滚、改源表后重试的快照隔离、跨批次时点隔离、半成品表回滚、
低权限角色写保护、模拟崩溃的重启恢复、并行执行观测、重试边界。

## 目录结构

```
db/init.sql            数据库初始化（表、角色、权限、种子数据与示例模型）
app/config.py          环境配置
app/db.py              连接池
app/sql_guard.py       SELECT-only 与引用合法性守卫
app/dag.py             无环校验 / 拓扑排序
app/scheduler.py       快照冻结、并行调度、失败阻塞、重试、重启恢复
app/services.py        模型 CRUD 与校验
app/runs_service.py    CSV 上传、批次/结果查询
app/main.py            FastAPI 路由与生命周期
app/web/               原生 JS 单页前端
tests/                 端到端与恢复测试
docker-compose.yml     db + api 两容器，带健康检查
```
