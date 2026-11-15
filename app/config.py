"""运行期配置：全部通过环境变量注入，由 docker-compose 提供默认值。"""
import os

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@db:5432/tryout",
)

# 模型上限
MAX_MODELS = 4
# 单条模型 SQL 语句超时（毫秒）
STATEMENT_TIMEOUT_MS = int(os.environ.get("STATEMENT_TIMEOUT_MS", "30000"))
# 节点并发上限（互不依赖的模型并行）
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "4"))
# 允许作为模型直接读取的源表
SOURCE_TABLES = ("sales", "refunds")
RUNNER_ROLE = "model_runner"
