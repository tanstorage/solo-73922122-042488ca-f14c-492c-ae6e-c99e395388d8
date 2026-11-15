-- ============================================================================
-- 模型试跑平台 - 数据库初始化
-- 由 postgres 官方镜像在首次启动时自动执行（仅对空数据卷生效）
-- ============================================================================

-- 1) 低权限执行角色：模型 SQL 以该角色身份运行，只能读源数据/快照、
--    在 run_snap 内写自己的结果表、更新试跑状态，无法改写平台表。
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'model_runner') THEN
    CREATE ROLE model_runner NOLOGIN;
  END IF;
END
$$;

-- 2) 应用库对象
CREATE SCHEMA IF NOT EXISTS public;

-- ---------------------------------------------------------------------------
-- 源表：预置 sales / refunds，结构可通过 CSV 覆盖更新
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.sales (
    id           INTEGER PRIMARY KEY,
    store_id     TEXT NOT NULL,
    biz_date     DATE NOT NULL,
    amount       NUMERIC(14, 2) NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.refunds (
    id           INTEGER PRIMARY KEY,
    store_id     TEXT NOT NULL,
    biz_date     DATE NOT NULL,
    refund_amount NUMERIC(14, 2) NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 源数据版本：每次 CSV 覆盖上传递增，用于展示试跑冻结的时点
CREATE SEQUENCE IF NOT EXISTS public.source_version_seq START 1;

-- ---------------------------------------------------------------------------
-- 模型定义（最多 4 个启用的模型）
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.models (
    id            SERIAL PRIMARY KEY,
    name          TEXT NOT NULL UNIQUE,
    sql_text      TEXT NOT NULL,
    upstream_ids  INTEGER[] NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- 试跑批次：一次试跑 = 一个不可变的数据时点
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.runs (
    id              SERIAL PRIMARY KEY,
    status          TEXT NOT NULL DEFAULT 'initializing'
                    CHECK (status IN ('initializing','running','succeeded','failed')),
    source_version  INTEGER NOT NULL DEFAULT 0,
    snapshot_schema TEXT,
    error_summary   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ
);

-- ---------------------------------------------------------------------------
-- 试跑节点：模型在某次试跑中的执行状态
-- 状态机: pending -> running -> succeeded | failed ; 上游失败 -> blocked
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.run_nodes (
    id           SERIAL PRIMARY KEY,
    run_id       INTEGER NOT NULL REFERENCES public.runs(id) ON DELETE CASCADE,
    model_id     INTEGER NOT NULL,
    model_name   TEXT NOT NULL,
    sql_text     TEXT NOT NULL,           -- 试跑启动时冻结的 SQL
    upstream_ids INTEGER[] NOT NULL DEFAULT '{}',
    position     INTEGER NOT NULL,        -- 拓扑序，仅用于稳定展示
    status       TEXT NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending','running','succeeded','failed','blocked')),
    result_table TEXT,                    -- 成功后写入: run_snap_<run>.m_<model>
    error        TEXT,
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    UNIQUE (run_id, model_id)
);
CREATE INDEX IF NOT EXISTS idx_run_nodes_run ON public.run_nodes(run_id);

-- 事件审计：记录每次状态流转，失败原因也在此留痕
CREATE TABLE IF NOT EXISTS public.run_events (
    id          SERIAL PRIMARY KEY,
    run_id      INTEGER NOT NULL REFERENCES public.runs(id) ON DELETE CASCADE,
    node_id     INTEGER,
    event_type  TEXT NOT NULL,
    message     TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_run_events_run ON public.run_events(run_id);

-- 3) 权限 -------------------------------------------------------------------
GRANT USAGE ON SCHEMA public TO model_runner;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO model_runner;
GRANT SELECT, UPDATE ON public.run_nodes TO model_runner;
-- 节点事务内以 runner 身份记录执行事件
GRANT SELECT, INSERT ON public.run_events TO model_runner;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO model_runner;
-- 写结果表的 schema 在每次试跑创建时单独 GRANT

-- 4) 预置源数据（门店日结场景示例）-----------------------------------------
INSERT INTO public.sales (id, store_id, biz_date, amount) VALUES
    (1, 'S01', DATE '2026-11-10', 1200.00),
    (2, 'S01', DATE '2026-11-11',  980.50),
    (3, 'S02', DATE '2026-11-10', 1560.00),
    (4, 'S02', DATE '2026-11-11', 2100.25),
    (5, 'S03', DATE '2026-11-11',  430.00)
ON CONFLICT (id) DO NOTHING;

INSERT INTO public.refunds (id, store_id, biz_date, refund_amount) VALUES
    (1, 'S01', DATE '2026-11-11',  80.50),
    (2, 'S02', DATE '2026-11-10',  60.00),
    (3, 'S02', DATE '2026-11-11', 100.25)
ON CONFLICT (id) DO NOTHING;

-- 5) 预置示例模型：日销售清洗 -> 净销售
INSERT INTO public.models (name, sql_text, upstream_ids) VALUES
    ('daily_sales_clean',
$SQL$
SELECT store_id,
       biz_date,
       SUM(amount) AS gross_amount
FROM sales
GROUP BY store_id, biz_date
$SQL$,
    '{}')
ON CONFLICT (name) DO NOTHING;

INSERT INTO public.models (name, sql_text, upstream_ids)
SELECT 'daily_net_sales',
$SQL$
SELECT s.store_id,
       s.biz_date,
       s.gross_amount - COALESCE(r.refund_amount, 0) AS net_amount
FROM daily_sales_clean s
LEFT JOIN (
    SELECT store_id, biz_date, SUM(refund_amount) AS refund_amount
    FROM refunds
    GROUP BY store_id, biz_date
) r ON r.store_id = s.store_id AND r.biz_date = s.biz_date
$SQL$,
       ARRAY[m.id]
FROM public.models m
WHERE m.name = 'daily_sales_clean'
ON CONFLICT (name) DO NOTHING;
