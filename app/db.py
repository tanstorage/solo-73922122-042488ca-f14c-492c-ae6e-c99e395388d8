"""PostgreSQL 连接与轻量连接池。

节点任务在线程池中执行，每个任务借用一条独立连接，用完归还。
"""
from __future__ import annotations

import queue
import threading
from contextlib import contextmanager
from typing import Iterator

import psycopg2
import psycopg2.extras  # noqa: F401  注册 dict cursor

from .config import DATABASE_URL

_pool: "queue.Queue" = queue.Queue(maxsize=16)
_pool_lock = threading.Lock()
_pool_size = 0


def _new_conn():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    # 服务重启恢复时可能残留 running 节点，事务由应用侧保证可整体回滚
    return conn


def get_conn():
    global _pool_size
    try:
        return _pool.get_nowait()
    except queue.Empty:
        with _pool_lock:
            if _pool_size < 16:
                _pool_size += 1
                return _new_conn()
    # 池满则等待归还
    return _pool.get()


def put_conn(conn) -> None:
    try:
        _pool.put_nowait(conn)
    except queue.Full:
        conn.close()


@contextmanager
def conn_ctx() -> Iterator:
    conn = get_conn()
    try:
        yield conn
    finally:
        # 防止遗留事务污染下一个借用者
        try:
            if conn.closed == 0:
                conn.rollback()
        except Exception:
            pass
        put_conn(conn)


def fetch_all(conn, sql: str, params=None):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()


def fetch_one(conn, sql: str, params=None):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params or ())
        return cur.fetchone()


def close_all() -> None:
    while True:
        try:
            conn = _pool.get_nowait()
        except queue.Empty:
            return
        try:
            conn.close()
        except Exception:
            pass
