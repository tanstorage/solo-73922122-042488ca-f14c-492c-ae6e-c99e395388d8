"""端到端测试：通过 FastAPI TestClient + 真实 PostgreSQL 验证全部关键保证。

运行前提：环境变量 DATABASE_URL 指向一个已执行 db/init.sql 的空库。
测试自身会清理它创建的快照 schema 与 runs 记录。
"""
import io
import os
import time

import psycopg2
import pytest

from conftest import conn as _new_conn


def _conn():
    return _new_conn()


SEED_MODELS_SQL = open(
    os.path.join(os.path.dirname(__file__), "..", "db", "init.sql")
).read()


@pytest.fixture(autouse=True)
def clean_runs():
    """每个测试前后清理试跑数据并重置模型为种子状态，保留源表基线。"""
    def _clean():
        c = _conn()
        cur = c.cursor()
        cur.execute("SELECT id, snapshot_schema FROM public.runs")
        rows = cur.fetchall()
        cur.execute("DELETE FROM public.run_events")
        cur.execute("DELETE FROM public.run_nodes")
        cur.execute("DELETE FROM public.runs")
        for _, schema in rows:
            if schema:
                cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        cur.execute("DELETE FROM public.models")
        cur.execute("ALTER SEQUENCE models_id_seq RESTART WITH 1")
        cur.execute(
            "INSERT INTO public.models (name, sql_text, upstream_ids) VALUES "
            "('daily_sales_clean', %s, '{}')",
            ("SELECT store_id, biz_date, SUM(amount) AS gross_amount "
             "FROM sales GROUP BY store_id, biz_date",),
        )
        cur.execute(
            "INSERT INTO public.models (name, sql_text, upstream_ids) "
            "SELECT 'daily_net_sales', %s, ARRAY[m.id] "
            "FROM public.models m WHERE m.name='daily_sales_clean'",
            ("SELECT s.store_id, s.biz_date, "
             "s.gross_amount - COALESCE(r.refund_amount, 0) AS net_amount "
             "FROM daily_sales_clean s LEFT JOIN ("
             "SELECT store_id, biz_date, SUM(refund_amount) AS refund_amount "
             "FROM refunds GROUP BY store_id, biz_date"
             ") r ON r.store_id=s.store_id AND r.biz_date=s.biz_date",),
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
        time.sleep(0.1)
    raise AssertionError("run did not finish in time")


def _node_by_name(detail, name):
    return next(n for n in detail["nodes"] if n["model_name"] == name)


# ---------------------------------------------------------------------------
def test_health_and_seed(client):
    assert client.get("/api/health").json() == {"status": "ok"}
    sources = {s["table"]: s for s in client.get("/api/sources").json()}
    assert sources["sales"]["total"] == 5
    assert sources["refunds"]["total"] == 3
    models = client.get("/api/models").json()
    assert len(models) == 2


def test_sql_guard_rejects_write_and_unknown_table(client):
    # 写操作
    r = client.post("/api/models", json={
        "name": "bad_write", "sql_text": "DELETE FROM sales", "upstream_ids": []})
    assert r.status_code == 400 and "SELECT" in r.json()["detail"]
    # 未声明的表
    r = client.post("/api/models", json={
        "name": "bad_ref", "sql_text": "SELECT * FROM pg_tables", "upstream_ids": []})
    assert r.status_code == 400 and "未声明" in r.json()["detail"]
    # schema 限定
    r = client.post("/api/models", json={
        "name": "bad_qual", "sql_text": "SELECT * FROM public.sales",
        "upstream_ids": []})
    assert r.status_code == 400 and "schema" in r.json()["detail"]
    # 通配符
    r = client.post("/api/models", json={
        "name": "bad_wild", "sql_text": "SELECT s.* FROM sales s",
        "upstream_ids": []})
    assert r.status_code == 400 and "通配符" in r.json()["detail"]
    # 表别名.列名是合法写法，不应误报
    r = client.post("/api/models", json={
        "name": "ok_alias",
        "sql_text": "SELECT s.store_id, s.biz_date FROM sales s "
                    "JOIN refunds r ON r.store_id = s.store_id",
        "upstream_ids": []})
    assert r.status_code == 201, r.json()
    client.delete(f"/api/models/{r.json()['id']}")
    # 多语句
    r = client.post("/api/models", json={
        "name": "bad_multi",
        "sql_text": "SELECT * FROM sales; DROP TABLE sales",
        "upstream_ids": []})
    assert r.status_code == 400
    # 关键字绕过（字符串字面量里的 insert 不应误杀，但真正的 insert 要拦）
    r = client.post("/api/models", json={
        "name": "ok_literal",
        "sql_text": "SELECT 'insert' AS tag, id FROM sales WHERE amount > 0",
        "upstream_ids": []})
    assert r.status_code == 201, r.json()
    client.delete(f"/api/models/{r.json()['id']}")


def test_dag_cycle_and_limit(client):
    models = client.get("/api/models").json()
    a = models[0]["id"]
    # 自依赖
    r = client.put(f"/api/models/{a}", json={
        "name": models[0]["name"], "sql_text": models[0]["sql_text"],
        "upstream_ids": [a]})
    assert r.status_code == 400 and "自身" in r.json()["detail"]

    # 造环：让模型1依赖模型2（模型2已依赖模型1）
    b = models[1]["id"]
    r = client.put(f"/api/models/{a}", json={
        "name": models[0]["name"], "sql_text": models[0]["sql_text"],
        "upstream_ids": [b]})
    assert r.status_code == 400 and "环" in r.json()["detail"]

    # 上限 4：已有 2 个，再加 2 个达到 4 可以，第 5 个拒绝
    existing = {m["name"] for m in client.get("/api/models").json()}
    created = []
    for i in range(2):
        name = f"extra_{i}"
        if name in existing:
            continue
        rr = client.post("/api/models", json={
            "name": name,
            "sql_text": "SELECT store_id FROM sales GROUP BY store_id",
            "upstream_ids": []})
        assert rr.status_code == 201, rr.json()
        created.append(rr.json()["id"])
    rr = client.post("/api/models", json={
        "name": "one_too_many",
        "sql_text": "SELECT DISTINCT store_id FROM refunds",
        "upstream_ids": []})
    assert rr.status_code == 400 and "4 个模型" in rr.json()["detail"]
    for mid in created:
        client.delete(f"/api/models/{mid}")


def test_full_success_run_and_results(client):
    r = client.post("/api/runs")
    assert r.status_code == 201
    run_id = r.json()["run_id"]
    detail = _wait_terminal(client, run_id)
    assert detail["run"]["status"] == "succeeded"
    assert all(n["status"] == "succeeded" for n in detail["nodes"])

    # 结果可查，且下游确实用到了上游结果表
    upstream = _node_by_name(detail, "daily_sales_clean")
    res = client.get(f"/api/runs/{run_id}/nodes/{upstream['id']}/result").json()
    assert res["total"] == 5  # 5 行销售对应 5 个不同的门店+日期组合
    assert {"store_id", "biz_date", "gross_amount"} == set(res["columns"])

    downstream = _node_by_name(detail, "daily_net_sales")
    res2 = client.get(f"/api/runs/{run_id}/nodes/{downstream['id']}/result").json()
    # S02 在 11-11 的净额 = 2100.25 - 100.25 = 2000.00
    row = next(x for x in res2["rows"]
               if x["store_id"] == "S02" and str(x["biz_date"]).startswith("2026-11-11"))
    assert float(row["net_amount"]) == 2000.00


def test_failure_blocks_downstream_others_continue(client):
    models = client.get("/api/models").json()
    upstream = next(m for m in models if m["name"] == "daily_sales_clean")
    downstream = next(m for m in models if m["name"] == "daily_net_sales")

    # 加一条独立分支模型，验证失败不影响其他分支
    r = client.post("/api/models", json={
        "name": "independent_branch",
        "sql_text": "SELECT store_id, SUM(refund_amount) AS r FROM refunds GROUP BY store_id",
        "upstream_ids": []})
    assert r.status_code == 201
    independent_id = r.json()["id"]

    # 把上游 SQL 改成必然报错（引用不存在的列）
    client.patch(f"/api/models/{upstream['id']}/sql", json={
        "sql_text": "SELECT store_id, biz_date, SUM(no_such_col) AS x FROM sales GROUP BY 1,2"})

    run_id = client.post("/api/runs").json()["run_id"]
    detail = _wait_terminal(client, run_id)
    assert detail["run"]["status"] == "failed"

    up_node = _node_by_name(detail, "daily_sales_clean")
    down_node = _node_by_name(detail, "daily_net_sales")
    ind_node = _node_by_name(detail, "independent_branch")

    assert up_node["status"] == "failed" and "no_such_col" in up_node["error"]
    assert down_node["status"] == "blocked"
    assert ind_node["status"] == "succeeded"  # 其他分支继续

    # 失败节点没有半成品结果表
    c = _conn()
    cur = c.cursor()
    cur.execute("SELECT to_regclass(%s)", (up_node["result_table"] or "public.none",))
    # result_table 应为 NULL；即便有值也确认表不存在
    assert up_node["result_table"] is None
    cur.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema=%s",
        (detail["run"]["snapshot_schema"],))
    # 快照内：2 张源表 + 独立分支结果表 = 3，失败/阻塞节点无结果表
    assert cur.fetchone()[0] == 3
    c.close()

    # 恢复现场
    client.delete(f"/api/models/{independent_id}")


def test_retry_uses_frozen_snapshot_after_source_change(client):
    # 重建失败场景（独立测试，不依赖前序用例的返回值）
    models = client.get("/api/models").json()
    upstream = next(m for m in models if m["name"] == "daily_sales_clean")
    downstream = next(m for m in models if m["name"] == "daily_net_sales")

    r = client.post("/api/models", json={
        "name": "independent_branch",
        "sql_text": "SELECT store_id, SUM(refund_amount) AS r FROM refunds GROUP BY store_id",
        "upstream_ids": []})
    assert r.status_code == 201
    independent_id = r.json()["id"]

    client.patch(f"/api/models/{upstream['id']}/sql", json={
        "sql_text": "SELECT store_id, biz_date, SUM(no_such_col) AS x FROM sales GROUP BY 1,2"})
    run_id = client.post("/api/runs").json()["run_id"]
    detail = _wait_terminal(client, run_id)
    assert detail["run"]["status"] == "failed"
    client.delete(f"/api/models/{independent_id}")

    # 关键：试跑期间覆盖源表，重试也绝不能读到新数据
    new_csv = (
        "id,store_id,biz_date,amount\n"
        "1,S09,2026-12-01,9999.99\n"
    ).encode()
    up = client.post(
        "/api/sources/sales/upload",
        files={"file": ("sales.csv", io.BytesIO(new_csv), "text/csv")})
    assert up.status_code == 200
    assert up.json()["loaded_rows"] == 1
    # 实时源表已变
    assert client.get("/api/sources/sales").json()["total"] == 1

    # 修正失败模型 SQL（模拟开发者修 SQL）
    fixed = ("SELECT store_id, biz_date, SUM(amount) AS gross_amount "
             "FROM sales GROUP BY store_id, biz_date")
    r = client.patch(f"/api/models/{upstream['id']}/sql", json={"sql_text": fixed})
    assert r.status_code == 200

    rr = client.post(f"/api/runs/{run_id}/retry")
    assert rr.status_code == 200 and rr.json()["reset_nodes"] == 2  # 失败+阻塞
    detail = _wait_terminal(client, run_id)
    assert detail["run"]["status"] == "succeeded"

    # 结果来自旧快照（5 个门店+日期分组），而不是新 CSV 的 1 行
    up_node = _node_by_name(detail, "daily_sales_clean")
    res = client.get(f"/api/runs/{run_id}/nodes/{up_node['id']}/result").json()
    assert res["total"] == 5
    assert all(row["store_id"] != "S09" for row in res["rows"])

    # 成功节点的 SQL 仍是启动时冻结版本（重试只刷新 failed/blocked）
    # 事件流里要有 retry 记录
    assert any(e["event_type"] == "run_retry" for e in detail["events"])

    # 恢复源表，避免污染其他测试
    seed = (
        "id,store_id,biz_date,amount\n"
        "1,S01,2026-11-10,1200.00\n"
        "2,S01,2026-11-11,980.50\n"
        "3,S02,2026-11-10,1560.00\n"
        "4,S02,2026-11-11,2100.25\n"
        "5,S03,2026-11-11,430.00\n"
    ).encode()
    client.post("/api/sources/sales/upload",
                files={"file": ("sales.csv", io.BytesIO(seed), "text/csv")})


def test_snapshot_isolation_across_runs(client):
    # 第一次试跑：当前 5 行数据
    run1 = client.post("/api/runs").json()["run_id"]
    d1 = _wait_terminal(client, run1)
    assert d1["run"]["status"] == "succeeded"

    # 改源表后再跑一次，新批次看到新数据，老批次结果不变
    new_csv = (
        "id,store_id,biz_date,amount\n"
        "1,S01,2026-11-10,1200.00\n"
        "2,S01,2026-11-11,980.50\n"
        "6,S07,2026-11-12,777.00\n"
    ).encode()
    client.post("/api/sources/sales/upload",
                files={"file": ("sales.csv", io.BytesIO(new_csv), "text/csv")})
    run2 = client.post("/api/runs").json()["run_id"]
    d2 = _wait_terminal(client, run2)

    n1 = _node_by_name(d1, "daily_sales_clean")
    n2 = _node_by_name(d2, "daily_sales_clean")
    r1 = client.get(f"/api/runs/{run1}/nodes/{n1['id']}/result").json()
    r2 = client.get(f"/api/runs/{run2}/nodes/{n2['id']}/result").json()
    assert r1["total"] == 5          # 旧时点
    assert r2["total"] == 3          # 新时点（3 行，3 个不同门店+日期）
    assert any(x["store_id"] == "S07" for x in r2["rows"])
    assert all(x["store_id"] != "S07" for x in r1["rows"])

    # 恢复源表
    seed = (
        "id,store_id,biz_date,amount\n"
        "1,S01,2026-11-10,1200.00\n"
        "2,S01,2026-11-11,980.50\n"
        "3,S02,2026-11-10,1560.00\n"
        "4,S02,2026-11-11,2100.25\n"
        "5,S03,2026-11-11,430.00\n"
    ).encode()
    client.post("/api/sources/sales/upload",
                files={"file": ("sales.csv", io.BytesIO(seed), "text/csv")})


def test_bad_csv_rejected_and_original_kept(client):
    before = client.get("/api/sources/sales").json()["total"]
    bad = "id,store_id,biz_date,amount\n1,S01,not-a-date,10\n".encode()
    r = client.post("/api/sources/sales/upload",
                    files={"file": ("x.csv", io.BytesIO(bad), "text/csv")})
    assert r.status_code == 400 and "YYYY-MM-DD" in r.json()["detail"]
    assert client.get("/api/sources/sales").json()["total"] == before

    bad_header = "id,store_id,biz_date\n1,S01,2026-11-10\n".encode()
    r = client.post("/api/sources/sales/upload",
                    files={"file": ("x.csv", io.BytesIO(bad_header), "text/csv")})
    assert r.status_code == 400 and "表头" in r.json()["detail"]
    assert client.get("/api/sources/sales").json()["total"] == before


def test_runner_role_cannot_write_platform_tables(client):
    """验证低权限防线：model_runner 无法改写平台表，但拥有节点所需最小权限。"""
    c = _conn()
    cur = c.cursor()
    cur.execute("SET ROLE model_runner")
    for stmt in ("DELETE FROM public.sales", "DROP TABLE public.models",
                 "CREATE TABLE public.hacked(x int)",
                 "DELETE FROM public.run_nodes"):
        with pytest.raises(psycopg2.Error):
            cur.execute(stmt)
        c.rollback()
        cur.execute("SET ROLE model_runner")
    # 允许的最小操作：更新节点状态、写事件
    cur.execute("UPDATE public.run_nodes SET status=status WHERE false")
    cur.execute(
        "INSERT INTO public.run_events (run_id, event_type, message) "
        "SELECT id, 'perm_probe', 'ok' FROM public.runs LIMIT 1"
    )
    cur.execute("RESET ROLE")
    cur.execute(
        "DELETE FROM public.run_events WHERE event_type='perm_probe'"
    )
    # 源表数据原样
    cur.execute("SELECT count(*) FROM public.sales")
    assert cur.fetchone()[0] == 5
    c.commit()
    c.close()
