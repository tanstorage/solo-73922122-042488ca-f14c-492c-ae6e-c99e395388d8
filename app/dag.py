"""DAG 工具：依赖校验、拓扑排序（Kahn 算法）、无环保证。"""
from __future__ import annotations

from .config import MAX_MODELS


class DagError(ValueError):
    pass


def topo_sort(models: list[dict]) -> list[dict]:
    """models: [{id, name, upstream_ids:[...]}]，返回按拓扑序排列的新列表。"""
    if len(models) > MAX_MODELS:
        raise DagError(f"最多只能定义 {MAX_MODELS} 个模型，当前 {len(models)} 个")

    by_id = {m["id"]: dict(m) for m in models}
    for m in models:
        for up in m["upstream_ids"]:
            if up not in by_id:
                raise DagError(
                    f"模型 {m['name']} 声明的上游 id={up} 不存在"
                )
            if up == m["id"]:
                raise DagError(f"模型 {m['name']} 不能依赖自身")

    indeg = {mid: 0 for mid in by_id}
    children: dict[int, list[int]] = {mid: [] for mid in by_id}
    for m in models:
        for up in m["upstream_ids"]:
            indeg[m["id"]] += 1
            children[up].append(m["id"])

    # 按 id 排序保证同层顺序稳定
    ready = sorted([mid for mid, d in indeg.items() if d == 0])
    order: list[dict] = []
    while ready:
        mid = ready.pop(0)
        order.append(by_id[mid])
        for child in sorted(children[mid]):
            indeg[child] -= 1
            if indeg[child] == 0:
                ready.append(child)
        ready.sort()

    if len(order) != len(by_id):
        cycle = [by_id[mid]["name"] for mid, d in indeg.items() if d > 0]
        raise DagError("依赖图存在环，涉及模型: " + ", ".join(cycle))
    return order


def reachable_downstream(model_id: int, models: list[dict]) -> set[int]:
    """返回以 model_id 为起点的全部下游（含自身）。"""
    children: dict[int, list[int]] = {}
    for m in models:
        for up in m["upstream_ids"]:
            children.setdefault(up, []).append(m["id"])
    seen = {model_id}
    stack = [model_id]
    while stack:
        cur = stack.pop()
        for ch in children.get(cur, []):
            if ch not in seen:
                seen.add(ch)
                stack.append(ch)
    return seen
