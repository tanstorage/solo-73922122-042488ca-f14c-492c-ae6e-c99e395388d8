"""试跑调度引擎。

核心保证
--------
1. 每次试跑启动时在独立 schema ``run_snap_<id>`` 内固化源表与模型 SQL，
   之后无论源表如何变化、服务是否重启，该次试跑永远只读到这个时点的数据；
2. 节点在单个事务内「建结果表 + 写状态」，失败整体回滚，不留半成品；
3. 上游失败 -> 下游 blocked，其余分支继续；并行只发生在互不依赖的节点间；
4. 重试仅重置 failed/blocked 节点，沿用同一快照与已成功上游；
5. 所有状态落库，服务重启后自动接管未完成的试跑（running 节点重新执行，
   因为其事务已随进程死亡而回滚，不会产生半成品或误报成功）。
"""
from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from . import db as dbmod
from .config import MAX_WORKERS, RUNNER_ROLE, SOURCE_TABLES, STATEMENT_TIMEOUT_MS
from .dag import topo_sort
from .sql_guard import validate_identifier

log = logging.getLogger("scheduler")

# 同一时刻只允许一个试跑批次在执行（避免多批次资源争用，语义更清晰）。
# 用 run_id 集合而非不可重入锁：批次若被外部清理，孤儿任务退出时不会卡死后续批次。
_active: set[int] = set()
# 主事件循环：同步 API 端点通过它调度后台任务
_main_loop: asyncio.AbstractEventLoop | None = None
_executor: ThreadPoolExecutor | None = None


def set_main_loop(loop: asyncio.AbstractEventLoop) -> None:
    global _main_loop
    _main_loop = loop


def _get_executor() -> ThreadPoolExecutor:
    global _executor
    if _executor is None:
        _executor = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="node")
    return _executor


# ---------------------------------------------------------------------------
# 试跑创建与快照
# ---------------------------------------------------------------------------
def create_run() -> dict:
    """创建试跑批次并冻结数据时点（同步事务，由 API 在线程中调用）。"""
    with dbmod.conn_ctx() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM public.runs "
            "WHERE status IN ('initializing','running') FOR UPDATE"
        )
        if cur.fetchone():
            raise RuntimeError("已有试跑正在进行，请等待其结束后再启动")

        cur.execute("SELECT * FROM public.models ORDER BY id")
        cols = [d[0] for d in cur.description]
        models = [dict(zip(cols, row)) for row in cur.fetchall()]
        if not models:
            raise RuntimeError("尚未定义任何模型，无法启动试跑")

        ordered = topo_sort(models)  # 无环校验，失败则整批不创建

        cur.execute("SELECT nextval('public.source_version_seq') AS ver")
        version = cur.fetchone()[0]

        cur.execute(
            "INSERT INTO public.runs (status, source_version) "
            "VALUES ('initializing', %s) RETURNING id",
            (version,),
        )
        run_id = cur.fetchone()[0]
        schema = f"run_snap_{run_id}"
        validate_identifier(schema)

        # 1) 建快照 schema，逐表 CTAS 冻结源数据
        cur.execute(f"CREATE SCHEMA {schema}")
        for tbl in SOURCE_TABLES:
            cur.execute(
                f"CREATE TABLE {schema}.{tbl} AS "
                f"SELECT * FROM public.{tbl} WITH NO DATA"
            )
            cur.execute(f"INSERT INTO {schema}.{tbl} SELECT * FROM public.{tbl}")

        # 2) 冻结模型节点（SQL 与依赖在启动时点固化）
        # 先全部插入拿到 node_id，再回填“节点级”上游 id，
        # 调度器与阻塞递归均以 node_id 为键（model_id 与 node_id 不同源）
        model_to_node: dict[int, int] = {}
        for pos, m in enumerate(ordered):
            cur.execute(
                "INSERT INTO public.run_nodes "
                "(run_id, model_id, model_name, sql_text, upstream_ids, position, status) "
                "VALUES (%s, %s, %s, %s, %s, %s, 'pending') RETURNING id",
                (run_id, m["id"], m["name"], m["sql_text"],
                 [], pos),
            )
            model_to_node[m["id"]] = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO public.run_events (run_id, event_type, message) "
                "VALUES (%s, 'run_created', %s)",
                (run_id, f"模型 {m['name']} 已冻结进试跑快照 {schema}"),
            )

        for m in ordered:
            node_upstreams = [
                model_to_node[uid] for uid in m["upstream_ids"] if uid in model_to_node
            ]
            cur.execute(
                "UPDATE public.run_nodes SET upstream_ids=%s "
                "WHERE run_id=%s AND model_id=%s",
                (node_upstreams, run_id, m["id"]),
            )

        cur.execute(
            "UPDATE public.runs SET snapshot_schema=%s, status='running', "
            "started_at=now() WHERE id=%s",
            (schema, run_id),
        )
        cur.execute(
            f"GRANT USAGE, CREATE ON SCHEMA {schema} TO {RUNNER_ROLE}"
        )
        # 源快照表只读；结果表由 runner 自行创建，天然为其属主
        cur.execute(
            f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {RUNNER_ROLE}"
        )
        conn.commit()
        return {"run_id": run_id, "snapshot_schema": schema}


# ---------------------------------------------------------------------------
# 节点执行（线程池内运行，独立连接、独立事务）
# ---------------------------------------------------------------------------
def _claim_node(conn, run_id: int, node_id: int) -> bool:
    """原子抢占：只有 pending 节点能被置为 running，避免重复执行。"""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE public.run_nodes SET status='running', started_at=now() "
            "WHERE id=%s AND run_id=%s AND status='pending' RETURNING id",
            (node_id, run_id),
        )
        claimed = cur.fetchone() is not None
        conn.commit()
        return claimed


def _execute_node(run_id: int, node: dict) -> None:
    schema = f"run_snap_{run_id}"
    # 结果表以模型名命名：下游模型 SQL 直接引用上游模型名即可命中
    result_table = node["model_name"]
    validate_identifier(result_table)
    qualified = f"{schema}.{result_table}"

    with dbmod.conn_ctx() as conn:
        if not _claim_node(conn, run_id, node["id"]):
            return  # 已被其他 worker 接管（重启场景）

        try:
            with conn.cursor() as cur:
                # search_path 指向快照：裸表名一律解析到冻结数据/上游结果
                cur.execute(
                    f"SET LOCAL statement_timeout = {int(STATEMENT_TIMEOUT_MS)}"
                )
                cur.execute(f"SET LOCAL ROLE {RUNNER_ROLE}")
                cur.execute(f"SET LOCAL search_path TO {schema}, public")
                # 结果表与状态在同一事务：失败即全部回滚，无半成品
                cur.execute(
                    f"CREATE TABLE {qualified} AS {node['sql_text']} WITH NO DATA"
                )
                cur.execute(f"INSERT INTO {qualified} {node['sql_text']}")
                # 以下状态/事件写入同样在降权角色内完成（已授最小权限）
                cur.execute(
                    "UPDATE public.run_nodes "
                    "SET status='succeeded', result_table=%s, error=NULL, "
                    "finished_at=now() WHERE id=%s",
                    (qualified, node["id"]),
                )
                cur.execute(
                    "INSERT INTO public.run_events (run_id, node_id, event_type, message) "
                    "VALUES (%s, %s, 'node_succeeded', %s)",
                    (run_id, node["id"],
                     f"模型 {node['model_name']} 执行成功，结果写入 {qualified}"),
                )
                cur.execute("RESET ROLE")
            conn.commit()
        except Exception as exc:
            conn.rollback()
            _fail_node(run_id, node, exc)


def _fail_node(run_id: int, node: dict, exc: Exception) -> None:
    """标记节点失败，并将其全部下游递归置为 blocked。"""
    err = str(exc).strip()[:2000]
    with dbmod.conn_ctx() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE public.run_nodes SET status='failed', error=%s, "
            "finished_at=now() WHERE id=%s AND status='running'",
            (err, node["id"]),
        )
        cur.execute(
            "INSERT INTO public.run_events (run_id, node_id, event_type, message) "
            "VALUES (%s, %s, 'node_failed', %s)",
            (run_id, node["id"],
             f"模型 {node['model_name']} 执行失败: {err}"),
        )
        cur.execute(
            """
            WITH RECURSIVE down AS (
                SELECT id FROM public.run_nodes
                WHERE run_id=%s AND %s = ANY(upstream_ids)
                UNION
                SELECT n.id FROM public.run_nodes n
                JOIN down d ON d.id = ANY(n.upstream_ids)
                WHERE n.run_id=%s
            )
            UPDATE public.run_nodes
            SET status='blocked',
                error='上游模型执行失败，节点被阻塞',
                finished_at=now()
            WHERE id IN (SELECT id FROM down)
              AND status IN ('pending','running')
            RETURNING id, model_name
            """,
            (run_id, node["id"], run_id),
        )
        for nid, name in cur.fetchall():
            cur.execute(
                "INSERT INTO public.run_events (run_id, node_id, event_type, message) "
                "VALUES (%s, %s, 'node_blocked', %s)",
                (run_id, nid, f"模型 {name} 因上游失败被阻塞"),
            )
        conn.commit()


# ---------------------------------------------------------------------------
# 批次调度循环：async 编排，阻塞式 DB/节点工作全部丢进线程池
# ---------------------------------------------------------------------------
async def _run_scheduler(run_id: int) -> None:
    loop = asyncio.get_running_loop()
    executor = _get_executor()
    try:
        while True:
            nodes = await loop.run_in_executor(executor, _load_nodes, run_id)
            if not nodes:
                # 批次已被外部清理（如测试回滚），直接退出，不更新任何状态
                log.info("run %s 已不存在，调度任务退出", run_id)
                return
            terminal = {"succeeded", "failed", "blocked"}
            if all(n["status"] in terminal for n in nodes):
                break

            by_id = {n["id"]: n for n in nodes}
            dispatch = []
            for n in nodes:
                if n["status"] != "pending":
                    continue
                ups = [by_id[uid] for uid in n["upstream_ids"] if uid in by_id]
                if any(u["status"] in ("failed", "blocked") for u in ups):
                    continue  # 失败节点的递归 CTE 会负责阻塞传播
                if all(u["status"] == "succeeded" for u in ups):
                    dispatch.append(n)

            if dispatch:
                # 同层互不依赖的节点在线程池中并行执行
                await asyncio.gather(
                    *(loop.run_in_executor(executor, _execute_node, run_id, n)
                      for n in dispatch),
                    return_exceptions=True,
                )
                continue

            if not any(n["status"] == "running" for n in nodes):
                # 无可派发、无运行中、又未终态：传播阻塞后收敛
                await loop.run_in_executor(executor, _block_orphans, run_id)
                break
            # 尚有运行中的节点，轮询等待
            await asyncio.sleep(0.2)

        await loop.run_in_executor(executor, _finalize_run, run_id)
    except Exception:
        log.exception("scheduler crashed for run %s", run_id)


def _load_nodes(run_id: int) -> list[dict]:
    with dbmod.conn_ctx() as conn:
        rows = dbmod.fetch_all(
            conn,
            "SELECT * FROM public.run_nodes WHERE run_id=%s ORDER BY position",
            (run_id,),
        )
    return [dict(r) for r in rows]


def _block_orphans(run_id: int) -> None:
    with dbmod.conn_ctx() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE public.run_nodes
            SET status='blocked', error='依赖未成功完成，节点被阻塞',
                finished_at=now()
            WHERE run_id=%s AND status IN ('pending','running')
            """,
            (run_id,),
        )
        conn.commit()


def _finalize_run(run_id: int) -> None:
    with dbmod.conn_ctx() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT status, count(*) FROM public.run_nodes "
            "WHERE run_id=%s GROUP BY status",
            (run_id,),
        )
        stats = dict(cur.fetchall())
        if stats.get("failed") or stats.get("blocked"):
            status, summary = "failed", (
                f"失败 {stats.get('failed', 0)} 个，"
                f"阻塞 {stats.get('blocked', 0)} 个，"
                f"成功 {stats.get('succeeded', 0)} 个"
            )
        else:
            status, summary = "succeeded", "全部模型执行成功"
        cur.execute(
            "UPDATE public.runs SET status=%s, error_summary=%s, finished_at=now() "
            "WHERE id=%s",
            (status, summary, run_id),
        )
        cur.execute(
            "INSERT INTO public.run_events (run_id, event_type, message) "
            "VALUES (%s, 'run_finished', %s)",
            (run_id, summary),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# 重试：只重跑 failed/blocked，沿用快照与成功上游
# ---------------------------------------------------------------------------
def reset_for_retry(run_id: int) -> int:
    """把失败/阻塞节点重置为 pending，并刷新为模型当前 SQL。返回重置数量。"""
    with dbmod.conn_ctx() as conn, conn.cursor() as cur:
        cur.execute("SELECT status FROM public.runs WHERE id=%s FOR UPDATE",
                    (run_id,))
        row = cur.fetchone()
        if row is None:
            raise RuntimeError(f"试跑 {run_id} 不存在")
        if row[0] != "failed":
            raise RuntimeError("只有失败的试跑可以重试")

        cur.execute(
            """
            UPDATE public.run_nodes n
            SET status='pending', sql_text=m.sql_text,
                error=NULL, result_table=NULL,
                started_at=NULL, finished_at=NULL
            FROM public.models m
            WHERE n.model_id=m.id AND n.run_id=%s
              AND n.status IN ('failed','blocked')
            RETURNING n.id, n.model_id
            """,
            (run_id,),
        )
        reset_rows = cur.fetchall()
        if not reset_rows:
            raise RuntimeError("没有可重试的失败节点")

        schema = f"run_snap_{run_id}"
        for _, model_id in reset_rows:
            # 结果表按模型名命名，查出名称后删除
            cur.execute(
                "SELECT model_name FROM public.run_nodes WHERE model_id=%s "
                "AND run_id=%s",
                (model_id, run_id),
            )
            model_name = cur.fetchone()[0]
            cur.execute(f"DROP TABLE IF EXISTS {schema}.{model_name}")

        cur.execute(
            "UPDATE public.runs SET status='running', error_summary=NULL, "
            "finished_at=NULL WHERE id=%s",
            (run_id,),
        )
        cur.execute(
            "INSERT INTO public.run_events (run_id, event_type, message) "
            "VALUES (%s, 'run_retry', %s)",
            (run_id, f"重试 {len(reset_rows)} 个失败/阻塞节点，沿用本次快照数据"),
        )
        conn.commit()
        return len(reset_rows)


# ---------------------------------------------------------------------------
# 生命周期管理
# ---------------------------------------------------------------------------
def start_run_async(run_id: int) -> None:
    if run_id in _active:
        raise RuntimeError("该试跑已在调度中，请勿重复启动")
    if _main_loop is None:
        raise RuntimeError("事件循环尚未就绪，无法调度试跑")

    async def _runner():
        try:
            await _run_scheduler(run_id)
        finally:
            _active.discard(run_id)

    _active.add(run_id)
    _main_loop.create_task(_runner())


def recover_on_startup() -> list[int]:
    """进程启动时接管未完成批次。

    running 节点的事务随旧进程死亡已回滚、结果表不存在，重置为 pending
    后重新执行；succeeded 节点的结果表已提交，原样保留，续跑不重算。
    """
    recovered = []
    with dbmod.conn_ctx() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM public.runs WHERE status IN ('initializing','running') "
            "ORDER BY id"
        )
        run_ids = [r[0] for r in cur.fetchall()]
        for rid in run_ids:
            cur.execute(
                "UPDATE public.run_nodes SET status='pending', started_at=NULL "
                "WHERE run_id=%s AND status='running'",
                (rid,),
            )
            cur.execute(
                "UPDATE public.runs SET status='running' WHERE id=%s", (rid,)
            )
            cur.execute(
                "INSERT INTO public.run_events (run_id, event_type, message) "
                "VALUES (%s, 'service_recovered', '服务重启，试跑自动续跑')",
                (rid,),
            )
            recovered.append(rid)
        conn.commit()
    for rid in recovered:
        start_run_async(rid)
    return recovered
