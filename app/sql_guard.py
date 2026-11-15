"""模型 SQL 安全守卫。

规则：
1. 只允许单条 SELECT / WITH(...) SELECT 语句；
2. 禁止任何写操作、DDL、事务控制、EXPLAIN/游标/复制等语句；
3. 禁止 schema 限定名（模型只能落在试跑快照 schema 的 search_path 内）；
4. 扫描语句引用的表名，必须是源表或声明的上游模型；
5. 运行期再以低权限角色 model_runner 执行，形成第二道防线。
"""
from __future__ import annotations

import re

# 出现即拒绝的关键字（按词边界匹配，字符串字面量在预处理阶段先被掩码）
FORBIDDEN_KEYWORDS = [
    "insert", "update", "delete", "merge", "truncate", "drop", "alter",
    "create", "grant", "revoke", "vacuum", "reindex", "analyze", "explain",
    "commit", "rollback", "begin", "start", "savepoint", "prepare",
    "execute", "deallocate", "declare", "fetch", "copy", "call", "do",
    "lock", "set", "reset", "discard", "checkpoint", "listen", "notify",
    "reassign", "security", "session_user",
]

# 标识符：字母/下划线开头，后跟字母数字下划线
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# 真实 schema 限定名：public.t / pg_catalog.x / information_schema.t / run_snap_1.t
# 注意 t.col 这种“表别名.列”是合法的，不能误杀；只拦已知 schema 前缀与通配符
_SCHEMA_QUALIFIED_RE = re.compile(
    r"\b(public|pg_catalog|information_schema|pg_toast|run_snap_\d+)\s*\."
    r"|\.\s*\*",
    re.IGNORECASE,
)
# FROM/JOIN 后的主表名（含可能的别名），子查询括号不匹配
_TABLE_RE = re.compile(
    r"\b(?:from|join)\s+([A-Za-z_][A-Za-z0-9_]*)", re.IGNORECASE
)
# CTE 定义名 WITH x AS ( ... )
_CTE_RE = re.compile(
    r"\bwith\s+([A-Za-z_][A-Za-z0-9_]*)\s+as\s*\(", re.IGNORECASE
)
# 字符串/数字/注释掩码，避免关键字绕过
_STR_LITERAL_RE = re.compile(r"'(?:''|[^'])*'")
_LINE_COMMENT_RE = re.compile(r"--[^\n]*")
_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)


class SqlValidationError(ValueError):
    pass


def _mask_literals(sql: str) -> str:
    sql = _BLOCK_COMMENT_RE.sub(" ", sql)
    sql = _LINE_COMMENT_RE.sub(" ", sql)
    sql = _STR_LITERAL_RE.sub(" ? ", sql)
    return sql


def validate_model_sql(
    sql_text: str,
    allowed_tables: set[str],
) -> set[str]:
    """校验模型 SQL，返回其实际引用的表名集合。"""
    if not sql_text or not sql_text.strip():
        raise SqlValidationError("SQL 不能为空")

    masked = _mask_literals(sql_text)
    stripped = masked.strip().rstrip(";").strip()

    # 去掉末尾分号后，内部不得再出现分号（禁止多语句）
    if ";" in stripped:
        raise SqlValidationError("只允许单条 SELECT 语句，检测到额外的分号")

    lowered = stripped.lower()
    if not (lowered.startswith("select") or lowered.startswith("with")):
        raise SqlValidationError("模型只能包含 SELECT 或 WITH ... SELECT 语句")

    if _SCHEMA_QUALIFIED_RE.search(stripped):
        raise SqlValidationError(
            "禁止使用 schema 限定名（如 public.sales）或通配符 t.*，"
            "请直接使用表名与显式列名"
        )

    for kw in FORBIDDEN_KEYWORDS:
        if re.search(rf"\b{kw}\b", lowered):
            raise SqlValidationError(f"SQL 中包含禁止的关键字: {kw.upper()}")

    cte_names = {m.group(1).lower() for m in _CTE_RE.finditer(stripped)}

    # WITH 语句结构校验：最后一个右括号之后必须接最终 SELECT
    if lowered.startswith("with"):
        depth = 0
        last_close_tail = ""
        for idx, ch in enumerate(stripped):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    last_close_tail = stripped[idx + 1:].strip(" ;").lower()
        if not last_close_tail.startswith("select"):
            raise SqlValidationError(
                "WITH 语句必须在 CTE 之后接最终的 SELECT 结果集"
            )
    referenced = set()
    for m in _TABLE_RE.finditer(stripped):
        name = m.group(1)
        if name.lower() in cte_names:
            continue  # CTE 内部引用，非物理表
        if not _IDENT_RE.match(name):
            raise SqlValidationError(f"非法表名: {name}")
        referenced.add(name.lower())

    unknown = referenced - {t.lower() for t in allowed_tables}
    if unknown:
        raise SqlValidationError(
            "引用了未声明的表/上游模型: "
            + ", ".join(sorted(unknown))
            + "；可用来源: " + ", ".join(sorted(allowed_tables))
        )
    return referenced


def validate_identifier(name: str) -> None:
    if not _IDENT_RE.match(name or ""):
        raise SqlValidationError(f"非法标识符: {name!r}")
