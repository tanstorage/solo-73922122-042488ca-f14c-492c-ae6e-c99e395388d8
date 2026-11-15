"""模型定义的 CRUD 与校验。"""
from __future__ import annotations

from . import db as dbmod
from .config import MAX_MODELS, SOURCE_TABLES
from .dag import DagError, topo_sort
from .sql_guard import SqlValidationError, validate_identifier, validate_model_sql


class ModelError(ValueError):
    pass


def _row_to_model(row: dict) -> dict:
    return {
        "id": row["id"],
        "name": row["name"],
        "sql_text": row["sql_text"],
        "upstream_ids": list(row["upstream_ids"] or []),
        "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
        "updated_at": row["updated_at"].isoformat() if row.get("updated_at") else None,
    }


def list_models() -> list[dict]:
    with dbmod.conn_ctx() as conn:
        rows = dbmod.fetch_all(conn, "SELECT * FROM public.models ORDER BY id")
    return [_row_to_model(r) for r in rows]


def _no_active_run(cur) -> None:
    cur.execute(
        "SELECT id FROM public.runs WHERE status IN ('initializing','running')"
    )
    row = cur.fetchone()
    if row:
        raise ModelError(
            f"试跑 #{row[0]} 正在执行：模型结构（名称/依赖）锁定，"
            "SQL 文本可修改并在下次重试时生效"
        )


def _validate_definition(name: str, sql_text: str, upstream_ids: list[int],
                         cur, self_id: int | None) -> None:
    validate_identifier(name)
    if name.lower() in {t.lower() for t in SOURCE_TABLES}:
        raise ModelError(f"模型名不能与源表 {name} 重名")
    if not isinstance(upstream_ids, list) or not all(
        isinstance(u, int) for u in upstream_ids
    ):
        raise ModelError("upstream_ids 必须是整数数组")
    if len(set(upstream_ids)) != len(upstream_ids):
        raise ModelError("上游依赖不能重复")

    cur.execute("SELECT id, name FROM public.models ORDER BY id")
    existing = {r[0]: r[1] for r in cur.fetchall()}
    for up in upstream_ids:
        if up not in existing:
            raise ModelError(f"上游模型 id={up} 不存在")
        if self_id is not None and up == self_id:
            raise ModelError("模型不能依赖自身")

    # 现有模型的真实依赖映射（在组装假设图之前取好）
    cur.execute("SELECT id, upstream_ids FROM public.models")
    dep_map = {r[0]: list(r[1] or []) for r in cur.fetchall()}

    # 组装“假设保存后”的全集做 DAG 与上限校验
    candidate = {"id": self_id or -1, "name": name,
                 "upstream_ids": upstream_ids}
    graph = [
        {"id": mid, "name": mname, "upstream_ids": list(dep_map.get(mid, []))}
        for mid, mname in existing.items() if mid != self_id
    ]
    graph.append(candidate)
    if len(graph) > MAX_MODELS:
        raise ModelError(f"最多只能定义 {MAX_MODELS} 个模型")
    try:
        topo_sort(graph)
    except DagError as exc:
        raise ModelError(str(exc)) from exc

    # SQL 可引用的表 = 源表 + 声明的上游模型名
    allowed = set(SOURCE_TABLES)
    allowed.update(existing[uid] for uid in upstream_ids if uid in existing)
    try:
        validate_model_sql(sql_text, allowed)
    except SqlValidationError as exc:
        raise ModelError(str(exc)) from exc


def create_model(name: str, sql_text: str, upstream_ids: list[int]) -> dict:
    with dbmod.conn_ctx() as conn, conn.cursor() as cur:
        _no_active_run(cur)
        cur.execute("SELECT 1 FROM public.models WHERE name=%s", (name,))
        if cur.fetchone():
            raise ModelError(f"模型名 {name} 已存在")
        _validate_definition(name, sql_text, upstream_ids, cur, None)
        cur.execute(
            "INSERT INTO public.models (name, sql_text, upstream_ids) "
            "VALUES (%s, %s, %s) RETURNING *",
            (name, sql_text, upstream_ids),
        )
        cols = [d[0] for d in cur.description]
        row = dict(zip(cols, cur.fetchone()))
        conn.commit()
    return _row_to_model(row)


def update_model(model_id: int, name: str, sql_text: str,
                 upstream_ids: list[int]) -> dict:
    with dbmod.conn_ctx() as conn, conn.cursor() as cur:
        _no_active_run(cur)
        cur.execute("SELECT * FROM public.models WHERE id=%s FOR UPDATE",
                    (model_id,))
        cols = [d[0] for d in cur.description]
        raw = cur.fetchone()
        if raw is None:
            raise ModelError(f"模型 id={model_id} 不存在")
        _validate_definition(name, sql_text, upstream_ids, cur, model_id)
        cur.execute(
            "UPDATE public.models SET name=%s, sql_text=%s, upstream_ids=%s, "
            "updated_at=now() WHERE id=%s RETURNING *",
            (name, sql_text, upstream_ids, model_id),
        )
        row = dict(zip(cols, cur.fetchone()))
        conn.commit()
    return _row_to_model(row)


def patch_sql(model_id: int, sql_text: str) -> dict:
    """仅更新 SQL 文本。试跑执行期间也允许，重试时刷新到失败节点。"""
    with dbmod.conn_ctx() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM public.models WHERE id=%s FOR UPDATE",
                    (model_id,))
        cols = [d[0] for d in cur.description]
        raw = cur.fetchone()
        if raw is None:
            raise ModelError(f"模型 id={model_id} 不存在")
        current = dict(zip(cols, raw))
        allowed = set(SOURCE_TABLES)
        cur.execute("SELECT name FROM public.models WHERE id = ANY(%s)",
                    (current["upstream_ids"],))
        allowed.update(r[0] for r in cur.fetchall())
        try:
            validate_model_sql(sql_text, allowed)
        except SqlValidationError as exc:
            raise ModelError(str(exc)) from exc
        cur.execute(
            "UPDATE public.models SET sql_text=%s, updated_at=now() "
            "WHERE id=%s RETURNING *",
            (sql_text, model_id),
        )
        row = dict(zip(cols, cur.fetchone()))
        conn.commit()
    return _row_to_model(row)


def delete_model(model_id: int) -> None:
    with dbmod.conn_ctx() as conn, conn.cursor() as cur:
        _no_active_run(cur)
        cur.execute("SELECT name FROM public.models WHERE id=%s", (model_id,))
        if cur.fetchone() is None:
            raise ModelError(f"模型 id={model_id} 不存在")
        cur.execute(
            "SELECT 1 FROM public.models WHERE %s = ANY(upstream_ids)",
            (model_id,),
        )
        if cur.fetchone():
            raise ModelError("该模型被其他模型依赖，不能删除，请先调整依赖")
        cur.execute("DELETE FROM public.models WHERE id=%s", (model_id,))
        conn.commit()
