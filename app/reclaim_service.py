"""历史成功批次的快照空间回收。

业务规则
--------
1. 只允许回收**非最新**且状态为 ``succeeded`` 的批次（最新成功批次必须保留，
   供工程师对照最近一次试跑结果）；
2. 只删除该批次专属 schema（``run_snap_<id>``）内的对象：冻结的源表与
   各节点结果表，最后删除已清空的 schema 本身；批次、节点、事件等元数据
   全部保留，并在 ``runs.reclaimed_at`` 上打标、写入回收事件；
3. 回收与结果查询交错时的语义由 PostgreSQL 锁保证：
   - 已受理的查询事务持有 ACCESS SHARE 锁，``DROP TABLE`` 会等待其结束，
     因此查询一定能拿到完整结果后回收才真正执行；
   - 回收提交后发起的新查询先读到 ``reclaimed_at`` 标记，直接得到
     “结果已回收、不可读取”的明确响应（HTTP 410）；
4. 多个 Web 实例同时回收同一批次时，行级锁
   ``SELECT ... FOR UPDATE`` 把请求串行化，只有第一个请求执行清理，
   其余请求拿到 409「已回收」，清理严格只发生一次；
5. 删除对象与回收标记在**同一个数据库事务**内提交：数据库失败或服务
   退出都会整体回滚，绝不可能出现“打了标记但表还在”或“表没了但未标记”。
"""
from __future__ import annotations

import logging

import psycopg2

from . import db as dbmod
from .config import RECLAIM_LOCK_TIMEOUT_MS
from .sql_guard import validate_identifier

log = logging.getLogger("reclaim")


class ReclaimError(ValueError):
    """回收前置条件不满足（409）或批次不存在（404）。"""


class ReclaimBusyError(Exception):
    """快照表被在途查询长时间锁定，回收超时未执行（503，可安全重试）。"""


class ReclaimedError(ValueError):
    """结果所属快照已被回收，调用方应返回 410 Gone。"""


def reclaim_run(run_id: int) -> dict:
    """回收指定批次的专属快照 schema。返回回收摘要字典。"""
    with dbmod.conn_ctx() as conn, conn.cursor() as cur:
        # 只让“等待锁”阶段受限：正常 DROP 本身是元数据操作，瞬间完成。
        # 超时抛 QueryCanceled，整个事务回滚，回收标记绝不会先于删除落库。
        cur.execute(
            f"SET LOCAL lock_timeout = {int(RECLAIM_LOCK_TIMEOUT_MS)}"
        )
        # 行锁串行化所有针对同一批次的回收尝试（跨多个 Web 实例同样有效）
        cur.execute(
            "SELECT id, status, snapshot_schema, reclaimed_at "
            "FROM public.runs WHERE id=%s FOR UPDATE",
            (run_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise ReclaimError(f"试跑 #{run_id} 不存在")
        status, schema, reclaimed_at = row[1], row[2], row[3]
        if reclaimed_at is not None:
            # 已被另一个实例/请求清理：幂等返回冲突，清理不会重复执行
            raise ReclaimError(f"试跑 #{run_id} 的快照已回收，无需重复操作")
        if status != "succeeded":
            raise ReclaimError("只有成功结束的试跑批次可以回收快照")

        # 最新批次（无论状态如何）禁止回收，保留最近一次试跑的完整结果
        cur.execute("SELECT max(id) FROM public.runs")
        latest_id = cur.fetchone()[0]
        if latest_id == run_id:
            raise ReclaimError("最新批次不允许回收，请先发起新的试跑")

        if not schema:
            raise ReclaimError(f"试跑 #{run_id} 缺少快照 schema 信息，无法回收")
        # schema 名由系统按 run_id 生成，这里再做一次标识符防御性校验
        validate_identifier(schema)

        # 枚举该批次专属 schema 内的全部表并逐张删除。
        # DROP 会等待持有 ACCESS SHARE 的在途结果查询结束——
        # 已受理的查询因此始终能读完完整结果。
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema=%s AND table_type='BASE TABLE' "
            "ORDER BY table_name",
            (schema,),
        )
        tables = [r[0] for r in cur.fetchall()]

        # 删除前主动探测锁占用：若某张快照表正被其他会话读取（或已有
        # 回收事务在排队等待），立即放弃本次回收，不把服务端线程挂在
        # 锁等待上。探测、删除与标记在同一事务内，故不会漏判后到的锁：
        # 后到查询只会排在本事务之后，等本事务提交后看到 reclaimed_at
        # 标记并得到 410。
        cur.execute(
            """
            SELECT count(*) FROM pg_locks l
            JOIN pg_class c ON c.oid = l.relation
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND l.locktype = 'relation'
              AND l.pid <> pg_backend_pid()
              AND l.granted
            """,
            (schema,),
        )
        if cur.fetchone()[0] > 0:
            conn.rollback()
            raise ReclaimBusyError(
                "快照正被在途查询或其他会话占用，本次回收已取消，请稍后重试"
            )

        try:
            for tbl in tables:
                validate_identifier(tbl)
                cur.execute(f"DROP TABLE IF EXISTS {schema}.{tbl}")

            # 专属 schema 已清空，一并删除以释放全部占用空间；
            # CASCADE 仅作用于该隔离 schema 内可能残留的其他对象。
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        except psycopg2.errors.LockNotAvailable as exc:
            # lock_timeout（受运行环境限制时可能不触发）的兜底
            conn.rollback()
            raise ReclaimBusyError(
                "快照仍被在途查询读取，回收等待超时已取消，请稍后重试"
            ) from exc

        # 回收标记与物理删除同事务提交：任何失败都整体回滚，标记与快照不脱节
        cur.execute(
            "UPDATE public.runs SET reclaimed_at=now() WHERE id=%s",
            (run_id,),
        )
        cur.execute(
            "INSERT INTO public.run_events (run_id, event_type, message) "
            "VALUES (%s, 'snapshot_reclaimed', %s)",
            (run_id,
             f"快照 {schema} 已回收：删除 {len(tables)} 张表并移除 schema，"
             "批次记录与事件保留供复盘，结果数据不再可读取"),
        )
        conn.commit()

    log.info("run %s reclaimed snapshot %s, dropped %s tables",
             run_id, schema, len(tables))
    return {
        "run_id": run_id,
        "snapshot_schema": schema,
        "dropped_tables": tables,
        "dropped_count": len(tables),
    }
