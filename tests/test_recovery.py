"""恢复、并行与重试边界的补充测试。"""
import time

import pytest
from fastapi.testclient import TestClient

from conftest import conn as _conn


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


def test_recovery_after_simulated_crash(client):
    """模拟进程崩溃：running 节点重置后续跑，已成功节点不重算。"""
    # 先建一条会失败的链：root 成功，子节点失败
    r = client.post("/api/models", json={
        "name": "will_fail",
        "sql_text": "SELECT store_id, no_such_col FROM root",
        "upstream_ids": [1],
    })
    assert r.status_code == 201
    run_id = client.post("/api/runs").json()["run_id"]
    d = _wait_terminal(client, run_id)
    assert d["run"]["status"] == "failed"

    # 人为制造“崩溃现场”：批次置回 running；
    # 只有未完成的节点（will_fail）会在崩溃时处于 running，
    # 已提交成功的 root 保持 succeeded（真实崩溃中其事务已提交）
    c = _conn()
    cur = c.cursor()
    cur.execute("UPDATE public.runs SET status='running', finished_at=NULL WHERE id=%s",
                (run_id,))
    cur.execute(
        "UPDATE public.run_nodes SET status='running' "
        "WHERE run_id=%s AND status='failed'",
        (run_id,),
    )
    c.commit()
    c.close()

    # 新进程启动恢复
    from app import scheduler
    recovered = scheduler.recover_on_startup()
    assert run_id in recovered
    d = _wait_terminal(client, run_id)
    # root 仍成功；will_fail 重新执行后仍失败（SQL 未修）
    statuses = {n["model_name"]: n["status"] for n in d["nodes"]}
    assert statuses == {"root": "succeeded", "will_fail": "failed"}
    assert any(e["event_type"] == "service_recovered" for e in d["events"])

    # 修正 SQL 后再“重启”一次，应能续跑到成功
    client.patch("/api/models/2/sql", json={
        "sql_text": "SELECT store_id, c + 1 AS c2 FROM root"})
    # 直接重试失败批次
    client.post(f"/api/runs/{run_id}/retry")
    d = _wait_terminal(client, run_id)
    assert d["run"]["status"] == "succeeded"
    statuses = {n["model_name"]: n["status"] for n in d["nodes"]}
    assert statuses == {"root": "succeeded", "will_fail": "succeeded"}

    # 等待后台任务完全退出，避免活跃集合残留污染后续测试
    from app import scheduler
    deadline = time.time() + 5
    while time.time() < deadline and scheduler._active:
        time.sleep(0.05)
    assert not scheduler._active


def test_parallel_independent_nodes_overlap(client):
    """三个互不依赖的节点应并行：在模型内用 pg_sleep 放大执行窗口，
    轮询 pg_stat_activity 观测同时出现多个慢查询。"""
    for i in range(3):
        r = client.post("/api/models", json={
            "name": f"slow_{i}",
            # 仅 1 行输出；pg_sleep 放在标量子查询里（::text 规避 void 伪类型），
            # 该行输出时必定求值，规划器无法消除
            "sql_text": (
                f"SELECT DISTINCT {i} AS k, store_id, "
                "(SELECT pg_sleep(2.0)::text) AS tick "
                "FROM sales WHERE store_id = 'S01' LIMIT 1"
            ),
            "upstream_ids": [],
        })
        assert r.status_code == 201, r.json()

    max_concurrent = 0
    run_id = client.post("/api/runs").json()["run_id"]
    deadline = time.time() + 15
    c = _conn()
    while time.time() < deadline:
        cur = c.cursor()
        # SET ROLE 后会话用户仍是登录用户 postgres，故按查询文本匹配
        cur.execute(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE query LIKE '%pg_sleep%' AND state='active'"
        )
        max_concurrent = max(max_concurrent, cur.fetchone()[0])
        d = client.get(f"/api/runs/{run_id}").json()
        if d["run"]["status"] in ("succeeded", "failed"):
            break
        time.sleep(0.02)
    c.close()
    d = _wait_terminal(client, run_id)
    assert d["run"]["status"] == "succeeded"
    assert max_concurrent >= 2, f"未观测到并行，最大并发={max_concurrent}"


def test_retry_only_resets_failed_and_blocked(client):
    # root(ok) -> bad(fail) -> grand(blocked)
    client.post("/api/models", json={
        "name": "bad", "sql_text": "SELECT * FROM root WHERE xyz=1",
        "upstream_ids": [1]})
    client.post("/api/models", json={
        "name": "grand", "sql_text": "SELECT * FROM bad",
        "upstream_ids": [2]})
    run_id = client.post("/api/runs").json()["run_id"]
    d = _wait_terminal(client, run_id)
    before = {n["model_name"]: n for n in d["nodes"]}
    assert before["root"]["status"] == "succeeded"
    assert before["bad"]["status"] == "failed"
    assert before["grand"]["status"] == "blocked"
    root_finished = before["root"]["finished_at"]

    # 修 bad，重试
    client.patch("/api/models/2/sql", json={
        "sql_text": "SELECT store_id, c AS c2 FROM root"})
    r = client.post(f"/api/runs/{run_id}/retry")
    assert r.json()["reset_nodes"] == 2
    d = _wait_terminal(client, run_id)
    after = {n["model_name"]: n for n in d["nodes"]}
    assert d["run"]["status"] == "succeeded"
    # root 未被重跑：finished_at 保持原值
    assert after["root"]["finished_at"] == root_finished
    assert after["bad"]["status"] == "succeeded"
    assert after["grand"]["status"] == "succeeded"


def test_retry_requires_failed_status(client):
    run_id = client.post("/api/runs").json()["run_id"]
    _wait_terminal(client, run_id)
    # 成功批次不能重试
    r = client.post(f"/api/runs/{run_id}/retry")
    assert r.status_code == 409


def test_no_partial_table_on_failure(client):
    client.post("/api/models", json={
        "name": "boom",
        "sql_text": "SELECT store_id, cast('not-num' AS int) AS x FROM sales",
        "upstream_ids": []})
    run_id = client.post("/api/runs").json()["run_id"]
    d = _wait_terminal(client, run_id)
    assert d["run"]["status"] == "failed"
    c = _conn()
    cur = c.cursor()
    cur.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema=%s AND table_name='boom'",
        (d["run"]["snapshot_schema"],),
    )
    assert cur.fetchone()[0] == 0  # 失败事务回滚，无半成品表
    c.close()
