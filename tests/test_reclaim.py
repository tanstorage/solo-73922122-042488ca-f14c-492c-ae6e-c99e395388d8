"""快照回收功能测试：回收规则、查询交错、并发幂等与崩溃一致性。

运行前提：环境变量 DATABASE_URL 指向一个已执行 db/init.sql 的库。
"""
import contextlib
import threading
import time

import psycopg2
import pytest

from conftest import conn as _new_conn

from app import reclaim_service


def _conn():
    return _new_conn()


@pytest.fixture(autouse=True)
def clean_runs():
    def _clean():
        c = _conn()
        cur = c.cursor()
        cur.execute("SELECT snapshot_schema FROM public.runs")
        for (schema,) in cur.fetchall():
            if schema:
                cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        cur.execute("DELETE FROM public.run_events")
        cur.execute("DELETE FROM public.run_nodes")
        cur.execute("DELETE FROM public.runs")
        cur.execute("DELETE FROM public.models")
        cur.execute("ALTER SEQUENCE models_id_seq RESTART WITH 1")
        cur.execute(
            "INSERT INTO public.models (name, sql_text, upstream_ids) "
            "VALUES ('root', 'SELECT store_id, count(*) AS c FROM sales GROUP BY store_id', '{}')"
        )
        c.commit()
        c.close()
    _clean()
    yield
    _clean()


def _wait_terminal(client, run_id, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        d = client.get(f"/api/runs/{run_id}").json()
        if d["run"]["status"] in ("succeeded", "failed"):
            return d
        time.sleep(0.05)
    raise AssertionError("timeout")


def _make_succeeded_run(client):
    run_id = client.post("/api/runs").json()["run_id"]
    d = _wait_terminal(client, run_id)
    assert d["run"]["status"] == "succeeded"
    return run_id, d


def _schema_exists(cur, schema):
    cur.execute(
        "SELECT 1 FROM information_schema.schemata WHERE schema_name=%s",
        (schema,),
    )
    return cur.fetchone() is not None


def _tables_in_schema(cur, schema):
    cur.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema=%s",
        (schema,),
    )
    return {r[0] for r in cur.fetchall()}


# ---------------------------------------------------------------------------
def test_reclaim_happy_path_keeps_metadata_drops_schema(client):
    run_id, d = _make_succeeded_run(client)
    _make_succeeded_run(client)  # 第二批，使第一批成为可回收的非最新批次
    schema = d["run"]["snapshot_schema"]
    node = d["nodes"][0]

    c = _conn()
    cur = c.cursor()
    assert _schema_exists(cur, schema)
    before_tables = _tables_in_schema(cur, schema)
    # 快照内含冻结源表 + 结果表
    assert before_tables == {"sales", "refunds", "root"}
    c.close()

    r = client.post(f"/api/runs/{run_id}/reclaim")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["dropped_count"] == 3
    assert set(body["dropped_tables"]) == {"sales", "refunds", "root"}

    c = _conn()
    cur = c.cursor()
    # 物理快照已不存在
    assert not _schema_exists(cur, schema)
    # 批次、节点、事件全部保留
    cur.execute("SELECT status, (reclaimed_at IS NOT NULL) AS flag FROM public.runs WHERE id=%s",
                (run_id,))
    status, flag = cur.fetchone()
    assert status == "succeeded" and flag is True
    cur.execute("SELECT count(*) FROM public.run_nodes WHERE run_id=%s", (run_id,))
    assert cur.fetchone()[0] == 1
    cur.execute(
        "SELECT event_type FROM public.run_events WHERE run_id=%s", (run_id,)
    )
    events = {r[0] for r in cur.fetchall()}
    assert "snapshot_reclaimed" in events
    c.close()

    # 详情仍可查看，且带回收标记
    detail = client.get(f"/api/runs/{run_id}").json()
    assert detail["run"]["reclaimed_at"]
    # 列表也带标记
    listed = next(r for r in client.get("/api/runs").json() if r["id"] == run_id)
    assert listed["reclaimed_at"]

    # 回收后的新查询明确告知不可读取（410 Gone）
    rr = client.get(f"/api/runs/{run_id}/nodes/{node['id']}/result")
    assert rr.status_code == 410
    assert "回收" in rr.json()["detail"] and "不可读取" in rr.json()["detail"]


def test_reclaim_only_non_latest_succeeded(client):
    # 最新成功批次禁止回收
    run_id, _ = _make_succeeded_run(client)
    r = client.post(f"/api/runs/{run_id}/reclaim")
    assert r.status_code == 409 and "最新批次" in r.json()["detail"]

    # 失败批次即使不是最新也禁止回收
    client.patch("/api/models/1/sql", json={
        "sql_text": "SELECT store_id, no_such_col FROM sales"})
    failed_id = client.post("/api/runs").json()["run_id"]
    _wait_terminal(client, failed_id)
    r = client.post(f"/api/runs/{failed_id}/reclaim")
    assert r.status_code == 409 and "成功" in r.json()["detail"]

    # 修复 SQL 后再跑一次成功批次，使最早的成功批次变为非最新
    client.patch("/api/models/1/sql", json={
        "sql_text": "SELECT store_id, count(*) AS c FROM sales GROUP BY store_id"})
    new_id, _ = _make_succeeded_run(client)
    # failed_id 仍不能回收（失败批次）
    r = client.post(f"/api/runs/{failed_id}/reclaim")
    assert r.status_code == 409 and "成功" in r.json()["detail"]
    # 新的最新批次不能回收
    r = client.post(f"/api/runs/{new_id}/reclaim")
    assert r.status_code == 409 and "最新批次" in r.json()["detail"]
    # 最早的成功批次现在可以回收
    r = client.post(f"/api/runs/{run_id}/reclaim")
    assert r.status_code == 200, r.text

    # 不存在的批次 -> 404
    assert client.post("/api/runs/99999/reclaim").status_code == 404


def test_reclaim_is_idempotent_and_concurrency_safe(client):
    run_id, _ = _make_succeeded_run(client)
    # 制造第二个成功批次，使 run_id 可回收
    _make_succeeded_run(client)

    barrier = threading.Barrier(4)
    results = []

    def worker():
        barrier.wait()
        # 每个线程使用独立的连接池视图：TestClient 请求在线程内执行，
        # 应用连接池本身线程安全
        r = client.post(f"/api/runs/{run_id}/reclaim")
        results.append(r.status_code)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 恰好一次成功，其余全部 409（已被回收）
    assert sorted(results) == sorted([200, 409, 409, 409]), results

    # 再次请求仍然是 409，绝不重复清理
    r = client.post(f"/api/runs/{run_id}/reclaim")
    assert r.status_code == 409 and "已回收" in r.json()["detail"]

    c = _conn()
    cur = c.cursor()
    cur.execute("SELECT (reclaimed_at IS NOT NULL) FROM public.runs WHERE id=%s", (run_id,))
    assert cur.fetchone()[0] is True
    cur.execute(
        "SELECT count(*) FROM public.run_events "
        "WHERE run_id=%s AND event_type='snapshot_reclaimed'",
        (run_id,),
    )
    assert cur.fetchone()[0] == 1  # 回收事件只写一次
    c.close()


def test_inflight_query_gets_full_result_then_reclaim_runs(client):
    """回收与结果查询交错：先受理的查询必须拿到完整结果；查询结束前
    回收不执行（503 可重试），结束后回收成功，之后的新查询得到 410。

    用一个独立的 psycopg2 连接手动模拟“已受理、尚未结束”的查询事务：
    它持有结果表的 ACCESS SHARE 锁。
    """
    run_id, d = _make_succeeded_run(client)
    _make_succeeded_run(client)  # 第二批，解除最新批次限制
    node = d["nodes"][0]
    table = node["result_table"]

    reader = _conn()
    rcur = reader.cursor()
    # 开启事务并执行查询但不提交/回滚：锁持续持有
    rcur.execute(f"SELECT * FROM {table}")
    expected_rows = rcur.fetchall()
    assert expected_rows

    # 查询事务打开期间：回收探测到在途读锁，立即取消而非强行 DROP
    busy = client.post(f"/api/runs/{run_id}/reclaim")
    assert busy.status_code == 503
    assert "在途查询" in busy.json()["detail"]

    # 在途查询事务仍可在同一快照里反复读取，结果一行不少
    rcur.execute(f"SELECT count(*) FROM {table}")
    assert rcur.fetchone()[0] == len(expected_rows)

    # 此时快照与标记都未变化，其他新查询仍能读到完整结果
    probe = client.get(f"/api/runs/{run_id}/nodes/{node['id']}/result")
    assert probe.status_code == 200
    assert probe.json()["total"] == len(expected_rows)

    # 释放在途查询后重试回收，一次成功
    reader.rollback()
    reader.close()
    ok = client.post(f"/api/runs/{run_id}/reclaim")
    assert ok.status_code == 200, ok.text
    assert ok.json()["dropped_count"] == 3

    # 回收提交后的新查询一律明确拒绝（410），不能 500 或读到空表
    gone = client.get(f"/api/runs/{run_id}/nodes/{node['id']}/result")
    assert gone.status_code == 410
    assert "回收" in gone.json()["detail"] and "不可读取" in gone.json()["detail"]


def test_reclaim_marker_and_snapshot_stay_consistent_on_failure(client, monkeypatch):
    """模拟数据库失败：回收整体回滚，标记与快照物理状态保持一致。"""
    run_id, d = _make_succeeded_run(client)
    _make_succeeded_run(client)
    schema = d["run"]["snapshot_schema"]

    # 让 DROP TABLE 执行时抛错，模拟数据库中途故障；连接对象不被修改，
    # 仅通过代理包装 cursor，故障事务由真实连接整体回滚。
    import app.db as dbmod

    class FailingCursor:
        def __init__(self, inner):
            self._inner = inner

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return self._inner.close()

        def execute(self, sql, params=None):
            text = sql if isinstance(sql, str) else sql.decode()
            if text.strip().upper().startswith("DROP TABLE"):
                raise psycopg2.OperationalError("simulated drop failure")
            return self._inner.execute(sql, params)

        def __getattr__(self, item):
            return getattr(self._inner, item)

    class FailingConn:
        def __init__(self, real):
            self._real = real

        def cursor(self, *a, **k):
            return FailingCursor(self._real.cursor(*a, **k))

        def __getattr__(self, item):
            return getattr(self._real, item)

    @contextlib.contextmanager
    def failing_conn_ctx():
        real = dbmod.get_conn()
        try:
            yield FailingConn(real)
        finally:
            try:
                real.rollback()
            finally:
                dbmod.put_conn(real)

    monkeypatch.setattr(reclaim_service.dbmod, "conn_ctx", failing_conn_ctx)
    with pytest.raises(psycopg2.OperationalError):
        reclaim_service.reclaim_run(run_id)
    monkeypatch.undo()

    # 故障后：没有回收标记，schema 与全部表都还在，数据仍可读
    c = _conn()
    cur = c.cursor()
    assert _schema_exists(cur, schema)
    assert _tables_in_schema(cur, schema) == {"sales", "refunds", "root"}
    cur.execute("SELECT reclaimed_at FROM public.runs WHERE id=%s", (run_id,))
    assert cur.fetchone()[0] is None
    c.close()

    node = d["nodes"][0]
    r = client.get(f"/api/runs/{run_id}/nodes/{node['id']}/result")
    assert r.status_code == 200 and r.json()["total"] >= 1

    # 故障恢复后可以正常重试回收，一次成功
    r = client.post(f"/api/runs/{run_id}/reclaim")
    assert r.status_code == 200, r.text
    c = _conn()
    cur = c.cursor()
    assert not _schema_exists(cur, schema)
    cur.execute("SELECT (reclaimed_at IS NOT NULL) FROM public.runs WHERE id=%s", (run_id,))
    assert cur.fetchone()[0] is True
    c.close()


def test_reclaim_blocked_while_reader_holds_lock_leaves_nothing_changed(client):
    """在途查询存在时回收被拒绝（503），且不做任何修改、可安全重试。"""
    run_id, d = _make_succeeded_run(client)
    _make_succeeded_run(client)
    schema = d["run"]["snapshot_schema"]
    table = d["nodes"][0]["result_table"]

    reader = _conn()
    rcur = reader.cursor()
    rcur.execute(f"SELECT * FROM {table}")  # 持锁不结束
    rcur.fetchall()

    r = client.post(f"/api/runs/{run_id}/reclaim")
    assert r.status_code == 503
    assert "在途查询" in r.json()["detail"]

    reader.rollback()
    reader.close()

    # 被取消后：无回收标记、schema 与全部表完好、没有回收事件、结果可读
    c = _conn()
    cur = c.cursor()
    assert _schema_exists(cur, schema)
    assert _tables_in_schema(cur, schema) == {"sales", "refunds", "root"}
    cur.execute("SELECT reclaimed_at FROM public.runs WHERE id=%s", (run_id,))
    assert cur.fetchone()[0] is None
    cur.execute(
        "SELECT count(*) FROM public.run_events "
        "WHERE run_id=%s AND event_type='snapshot_reclaimed'",
        (run_id,),
    )
    assert cur.fetchone()[0] == 0
    c.close()
    node = d["nodes"][0]
    assert client.get(
        f"/api/runs/{run_id}/nodes/{node['id']}/result").status_code == 200

    # 查询结束后重试回收成功
    r = client.post(f"/api/runs/{run_id}/reclaim")
    assert r.status_code == 200, r.text
    c = _conn()
    cur = c.cursor()
    assert not _schema_exists(cur, schema)
    cur.execute("SELECT (reclaimed_at IS NOT NULL) FROM public.runs WHERE id=%s", (run_id,))
    assert cur.fetchone()[0] is True
    c.close()


def test_failed_run_retry_unchanged_and_keeps_own_snapshot(client):
    """失败批次凭原快照重试的既有语义不受回收功能影响；
    且回收只动目标批次自己的 schema。"""
    # 两个成功批次 + 一个失败批次
    ok1, d1 = _make_succeeded_run(client)
    client.patch("/api/models/1/sql", json={
        "sql_text": "SELECT store_id, cast('x' AS int) FROM sales"})
    failed_id = client.post("/api/runs").json()["run_id"]
    _wait_terminal(client, failed_id)
    client.patch("/api/models/1/sql", json={
        "sql_text": "SELECT store_id, count(*) AS c FROM sales GROUP BY store_id"})
    ok2, d2 = _make_succeeded_run(client)

    schema1 = d1["run"]["snapshot_schema"]
    schema2 = d2["run"]["snapshot_schema"]

    # 回收最早批次，不影响其他批次的快照
    r = client.post(f"/api/runs/{ok1}/reclaim")
    assert r.status_code == 200
    c = _conn()
    cur = c.cursor()
    assert not _schema_exists(cur, schema1)
    assert _schema_exists(cur, schema2)
    # 失败批次的快照（含冻结源表）完好，重试可继续使用
    cur.execute("SELECT snapshot_schema FROM public.runs WHERE id=%s", (failed_id,))
    failed_schema = cur.fetchone()[0]
    assert _tables_in_schema(cur, failed_schema) == {"sales", "refunds"}
    c.close()

    # 失败批次照常凭原快照重试成功
    rr = client.post(f"/api/runs/{failed_id}/retry")
    assert rr.status_code == 200
    d = _wait_terminal(client, failed_id)
    assert d["run"]["status"] == "succeeded"
    c = _conn()
    cur = c.cursor()
    assert _tables_in_schema(cur, failed_schema) == {"sales", "refunds", "root"}
    c.close()
