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
# 数据库连接池容量：回收事务可能因等待在途查询锁而短暂占用连接，
# 容量需要高于并发工作线程数，避免查询与回收相互等待造成饥饿
POOL_MAXSIZE = int(os.environ.get("POOL_MAXSIZE", "32"))
# 回收事务等待在途查询释放快照表锁的最长时间（毫秒）。
# 超时后回收整体回滚、返回 503，由用户稍后重试；标记与快照不会出现不一致。
RECLAIM_LOCK_TIMEOUT_MS = int(os.environ.get("RECLAIM_LOCK_TIMEOUT_MS", "10000"))
# 允许作为模型直接读取的源表
SOURCE_TABLES = ("sales", "refunds")
RUNNER_ROLE = "model_runner"
