FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt /srv/requirements.txt
RUN pip install -r /srv/requirements.txt

COPY app /srv/app

EXPOSE 8000

# 单容器多 worker 时，每个进程启动都会执行 recover_on_startup；
# 节点抢占（pending->running 原子更新）保证同一节点不会被重复执行。
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
