"""源表 CSV 上传与试跑结果查询。"""
from __future__ import annotations

import csv
import io

from . import db as dbmod
from .config import SOURCE_TABLES

# 源表允许通过 CSV 写入的列（updated_at 由数据库维护）
TABLE_COLUMNS: dict[str, list[str]] = {
    "sales": ["id", "store_id", "biz_date", "amount"],
    "refunds": ["id", "store_id", "biz_date", "refund_amount"],
}
NUMERIC_COLUMNS = {"amount", "refund_amount"}
INT_COLUMNS = {"id"}
DATE_COLUMNS = {"biz_date"}


class UploadError(ValueError):
    pass


def preview_source(table: str, limit: int = 50) -> dict:
    if table not in TABLE_COLUMNS:
        raise UploadError(f"未知源表 {table}，可选: {', '.join(TABLE_COLUMNS)}")
    limit = max(1, min(limit, 500))
    with dbmod.conn_ctx() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT count(*) AS c FROM public.{table}")
        total = cur.fetchone()[0]
        cur.execute(
            f"SELECT {', '.join(TABLE_COLUMNS[table])} FROM public.{table} "
            f"ORDER BY id LIMIT %s",
            (limit,),
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        for r in rows:
            if "biz_date" in r and r["biz_date"] is not None:
                r["biz_date"] = r["biz_date"].isoformat()
    return {"table": table, "total": total, "columns": cols, "rows": rows}


def replace_with_csv(table: str, raw_bytes: bytes) -> dict:
    """整体覆盖源表：单事务内 TRUNCATE + COPY，失败回滚保持原数据。

    已启动的试跑快照不受影响——快照在独立 schema 内，永不回写。
    """
    if table not in TABLE_COLUMNS:
        raise UploadError(f"未知源表 {table}，可选: {', '.join(TABLE_COLUMNS)}")

    try:
        text = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise UploadError("CSV 必须是 UTF-8 编码") from exc

    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        raise UploadError("CSV 文件为空")
    header = [h.strip() for h in header]

    expected = TABLE_COLUMNS[table]
    if header != expected:
        raise UploadError(
            f"{table} 的 CSV 表头必须且仅包含: {', '.join(expected)}；"
            f"实际: {', '.join(header)}"
        )

    # 全量读入并做类型预检，避免脏数据进入事务
    records: list[tuple] = []
    seen_ids: set[int] = set()
    for line_no, row in enumerate(reader, start=2):
        if not row or all(not c.strip() for c in row):
            continue
        if len(row) != len(expected):
            raise UploadError(f"第 {line_no} 行列数 {len(row)} 与表头 {len(expected)} 不一致")
        record = []
        for col, val in zip(expected, (c.strip() for c in row)):
            if col in INT_COLUMNS:
                try:
                    val = int(val)
                except ValueError as exc:
                    raise UploadError(f"第 {line_no} 行 {col} 不是整数: {val}") from exc
                if col == "id":
                    if val in seen_ids:
                        raise UploadError(f"第 {line_no} 行 id={val} 重复")
                    seen_ids.add(val)
            elif col in NUMERIC_COLUMNS:
                try:
                    val = float(val)
                except ValueError as exc:
                    raise UploadError(f"第 {line_no} 行 {col} 不是数字: {val}") from exc
            elif col in DATE_COLUMNS:
                import datetime as _dt
                try:
                    val = _dt.date.fromisoformat(val)
                except ValueError as exc:
                    raise UploadError(
                        f"第 {line_no} 行 {col} 日期格式应为 YYYY-MM-DD: {val}"
                    ) from exc
            record.append(val)
        records.append(tuple(record))

    if not records:
        raise UploadError("CSV 没有任何数据行")

    cols_sql = ", ".join(expected)
    with dbmod.conn_ctx() as conn, conn.cursor() as cur:
        cur.execute(f"TRUNCATE public.{table} RESTART IDENTITY")
        mogrified = b",".join(
            cur.mogrify(
                f"({', '.join(['%s'] * len(expected))})", rec
            ) for rec in records
        )
        cur.execute(
            b"INSERT INTO public." + table.encode()
            + b" (" + cols_sql.encode() + b") VALUES " + mogrified
        )
        cur.execute("SELECT nextval('public.source_version_seq')")
        version = cur.fetchone()[0]
        conn.commit()
    return {"table": table, "loaded_rows": len(records), "source_version": version}


# ---------------------------------------------------------------------------
# 试跑查询
# ---------------------------------------------------------------------------
def list_runs(limit: int = 20) -> list[dict]:
    with dbmod.conn_ctx() as conn:
        rows = dbmod.fetch_all(
            conn,
            """
            SELECT r.id, r.status, r.source_version, r.snapshot_schema,
                   r.error_summary, r.created_at, r.started_at, r.finished_at,
                   count(n.*) AS node_total,
                   count(n.*) FILTER (WHERE n.status='succeeded') AS node_ok,
                   count(n.*) FILTER (WHERE n.status='failed') AS node_failed,
                   count(n.*) FILTER (WHERE n.status='blocked') AS node_blocked,
                   count(n.*) FILTER (WHERE n.status='running') AS node_running,
                   count(n.*) FILTER (WHERE n.status='pending') AS node_pending
            FROM public.runs r
            LEFT JOIN public.run_nodes n ON n.run_id = r.id
            GROUP BY r.id ORDER BY r.id DESC LIMIT %s
            """,
            (max(1, min(limit, 100)),),
        )
    out = []
    for r in rows:
        d = dict(r)
        for k in ("created_at", "started_at", "finished_at"):
            if d.get(k):
                d[k] = d[k].isoformat()
        out.append(d)
    return out


def get_run_detail(run_id: int) -> dict:
    with dbmod.conn_ctx() as conn:
        run = dbmod.fetch_one(
            conn, "SELECT * FROM public.runs WHERE id=%s", (run_id,)
        )
        if run is None:
            raise UploadError(f"试跑 #{run_id} 不存在")
        nodes = dbmod.fetch_all(
            conn,
            "SELECT * FROM public.run_nodes WHERE run_id=%s ORDER BY position",
            (run_id,),
        )
        events = dbmod.fetch_all(
            conn,
            "SELECT id, node_id, event_type, message, created_at "
            "FROM public.run_events WHERE run_id=%s ORDER BY id",
            (run_id,),
        )
    run_d = dict(run)
    for k in ("created_at", "started_at", "finished_at"):
        if run_d.get(k):
            run_d[k] = run_d[k].isoformat()
    node_out = []
    for n in nodes:
        d = dict(n)
        d["upstream_ids"] = list(d["upstream_ids"] or [])
        for k in ("started_at", "finished_at"):
            if d.get(k):
                d[k] = d[k].isoformat()
        node_out.append(d)
    event_out = []
    for e in events:
        d = dict(e)
        d["created_at"] = d["created_at"].isoformat()
        event_out.append(d)
    return {"run": run_d, "nodes": node_out, "events": event_out}


def get_node_result(run_id: int, node_id: int, limit: int = 200) -> dict:
    limit = max(1, min(limit, 1000))
    with dbmod.conn_ctx() as conn:
        node = dbmod.fetch_one(
            conn,
            "SELECT * FROM public.run_nodes WHERE run_id=%s AND id=%s",
            (run_id, node_id),
        )
        if node is None:
            raise UploadError(f"试跑 #{run_id} 中不存在节点 #{node_id}")
        if node["status"] != "succeeded" or not node["result_table"]:
            return {
                "node_id": node_id,
                "model_name": node["model_name"],
                "status": node["status"],
                "columns": [],
                "rows": [],
                "message": "节点未成功执行，暂无结果",
            }
        table = node["result_table"]  # 来自受控标识符，非用户自由输入
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) AS c FROM {table}")
            total = cur.fetchone()[0]
            cur.execute(f"SELECT * FROM {table} LIMIT %s", (limit,))
            cols = [d[0] for d in cur.description]
            raw_rows = cur.fetchall()
    rows = []
    for r in raw_rows:
        d = dict(zip(cols, r))
        for k, v in list(d.items()):
            if hasattr(v, "isoformat"):
                d[k] = v.isoformat()
            elif isinstance(v, (bytes,)):
                d[k] = str(v)
        rows.append(d)
    return {
        "node_id": node_id,
        "model_name": node["model_name"],
        "status": node["status"],
        "result_table": table,
        "total": total,
        "columns": cols,
        "rows": rows,
    }
