"""FastAPI 入口：模型管理、源表 CSV、试跑启动/重试/查询，以及静态页面。"""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from . import runs_service, scheduler, services
from .config import SOURCE_TABLES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 捕获主事件循环，供同步 API 端点调度后台试跑任务
    scheduler.set_main_loop(asyncio.get_running_loop())
    # 等待数据库就绪并接管上次未完成的试跑
    await _wait_for_db()
    recovered = scheduler.recover_on_startup()
    if recovered:
        log.info("重启恢复，续跑试跑: %s", recovered)
    yield


async def _wait_for_db(max_attempts: int = 30) -> None:
    import time

    from . import db as dbmod

    for attempt in range(1, max_attempts + 1):
        try:
            with dbmod.conn_ctx() as conn, conn.cursor() as cur:
                cur.execute("SELECT 1")
            return
        except Exception as exc:  # 容器启动时 db 可能尚未就绪
            log.info("等待数据库就绪 (%s/%s): %s", attempt, max_attempts, exc)
            time.sleep(2)
    raise RuntimeError("数据库在限定时间内未就绪")


app = FastAPI(title="模型试跑平台", version="1.0.0", lifespan=lifespan)


# ---------------------------------------------------------------------------
# 请求模型
# ---------------------------------------------------------------------------
class ModelIn(BaseModel):
    name: str = Field(min_length=1, max_length=63)
    sql_text: str = Field(min_length=1)
    upstream_ids: list[int] = Field(default_factory=list)


class SqlPatchIn(BaseModel):
    sql_text: str = Field(min_length=1)


# ---------------------------------------------------------------------------
# 模型 CRUD
# ---------------------------------------------------------------------------
@app.get("/api/models")
def api_list_models():
    return services.list_models()


@app.post("/api/models", status_code=201)
def api_create_model(body: ModelIn):
    try:
        return services.create_model(body.name, body.sql_text, body.upstream_ids)
    except services.ModelError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.put("/api/models/{model_id}")
def api_update_model(model_id: int, body: ModelIn):
    try:
        return services.update_model(
            model_id, body.name, body.sql_text, body.upstream_ids
        )
    except services.ModelError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.patch("/api/models/{model_id}/sql")
def api_patch_sql(model_id: int, body: SqlPatchIn):
    """试跑期间只允许改 SQL（供失败后修正重试），结构变更需等批次结束。"""
    try:
        return services.patch_sql(model_id, body.sql_text)
    except services.ModelError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.delete("/api/models/{model_id}", status_code=204)
def api_delete_model(model_id: int):
    try:
        services.delete_model(model_id)
    except services.ModelError as exc:
        raise HTTPException(400, str(exc)) from exc


# ---------------------------------------------------------------------------
# 源表与 CSV 上传
# ---------------------------------------------------------------------------
@app.get("/api/sources")
def api_list_sources():
    return [runs_service.preview_source(t, limit=20) for t in SOURCE_TABLES]


@app.get("/api/sources/{table}")
def api_preview_source(table: str):
    try:
        return runs_service.preview_source(table)
    except runs_service.UploadError as exc:
        raise HTTPException(400, str(exc)) from exc


@app.post("/api/sources/{table}/upload")
async def api_upload_csv(table: str, file: UploadFile = File(...)):
    raw = await file.read()
    if not raw:
        raise HTTPException(400, "上传文件为空")
    try:
        return runs_service.replace_with_csv(table, raw)
    except runs_service.UploadError as exc:
        raise HTTPException(400, str(exc)) from exc


# ---------------------------------------------------------------------------
# 试跑
# ---------------------------------------------------------------------------
@app.post("/api/runs", status_code=201)
def api_start_run():
    try:
        info = scheduler.create_run()
    except RuntimeError as exc:
        raise HTTPException(409, str(exc)) from exc
    scheduler.start_run_async(info["run_id"])
    return {"run_id": info["run_id"], **info}


@app.get("/api/runs")
def api_list_runs():
    return runs_service.list_runs()


@app.get("/api/runs/{run_id}")
def api_run_detail(run_id: int):
    try:
        return runs_service.get_run_detail(run_id)
    except runs_service.UploadError as exc:
        raise HTTPException(404, str(exc)) from exc


@app.post("/api/runs/{run_id}/retry", status_code=200)
def api_retry(run_id: int):
    try:
        reset = scheduler.reset_for_retry(run_id)
    except RuntimeError as exc:
        raise HTTPException(409, str(exc)) from exc
    scheduler.start_run_async(run_id)
    return {"run_id": run_id, "reset_nodes": reset}


@app.get("/api/runs/{run_id}/nodes/{node_id}/result")
def api_node_result(run_id: int, node_id: int):
    try:
        return runs_service.get_node_result(run_id, node_id)
    except runs_service.UploadError as exc:
        raise HTTPException(404, str(exc)) from exc


@app.get("/api/health")
def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# 静态前端
# ---------------------------------------------------------------------------
WEB_DIR = Path(__file__).parent / "web"
app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")


@app.get("/")
def index():
    return FileResponse(WEB_DIR / "index.html")
