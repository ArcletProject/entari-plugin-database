import json
import hashlib
from dataclasses import dataclass
from collections.abc import Callable, Iterable
from threading import RLock
from typing import Any, Literal

from alembic.migration import MigrationContext
from alembic.autogenerate import api as autogen_api
from alembic.operations import Operations
from alembic.operations import ops as alembic_ops
from alembic.operations.ops import (
    DropConstraintOp,
    CreateUniqueConstraintOp,
    AddConstraintOp,
    AlterColumnOp,
    DropColumnOp,
    CreateForeignKeyOp,
)

from graia.amnesia.builtins.sqla.model import Base
from graia.amnesia.builtins.sqla.service import SqlalchemyService
from arclet.entari.localdata import local_data
from sqlalchemy import MetaData
from sqlalchemy.schema import Table

from .utils import logger

_STATE_FILE = local_data.get_data_file("database", "migrations_lock.json")
_MODULE_MODELS: dict[str, dict[str, type[Base]]] = {}
_LOCK = RLock()


@dataclass
class CustomMigration:
    script_id: str
    script_rev: str
    replace: bool
    run_always: bool
    upgrade: Callable[[Operations, MigrationContext, str], None]
    downgrade: Callable[[Operations, MigrationContext, str], None] | None = None


_CUSTOM_MIGRATIONS: dict[str, list[CustomMigration]] = {}


def register_custom_migration(
    model_or_table: str | type[Base],
    type: Literal["upgrade", "downgrade"] = "upgrade",
    *,
    script_id: str | None = None,
    script_rev: str = "1",
    replace: bool = True,
    run_always: bool = False,
):
    """
    注册自定义迁移脚本。

    Args:
        model_or_table: 目标 ORM 模型或表名
        type: 脚本类型，默认为 "upgrade"。如果需要注册降级脚本，请传入 "downgrade" 并提供 downgrade 函数。
        script_id: 目标脚本标识，若需要 downgrade 则连同 upgrade 一起传入相同标识
        script_rev: 目标脚本版本，变化触发 upgrade/downgrade
        replace: True 时跳过该表自动结构迁移
        run_always: 每次都会执行 upgrade（仍记录版本）
    """
    if isinstance(model_or_table, str):
        table_name = model_or_table
    else:
        table_name = getattr(model_or_table, "__tablename__", None)
        if not table_name:
            raise ValueError("无法确定表名, 请传入 ORM 模型或表名")

    def wrapper(func: Callable[[Operations, MigrationContext, str], None]):
        nonlocal script_id
        if script_id is None:
            script_id = func.__name__ or "anonymous"
        with _LOCK:
            if type == "upgrade":
                _CUSTOM_MIGRATIONS.setdefault(table_name, []).append(
                    CustomMigration(
                        script_id=script_id,
                        script_rev=str(script_rev),
                        replace=replace,
                        run_always=run_always,
                        upgrade=func,
                    )
                )
            else:
                if not script_id:
                    raise ValueError("注册 downgrade 脚本必须提供 script_id")
                if table_name not in _CUSTOM_MIGRATIONS:
                    raise ValueError("必须先注册 upgrade 脚本后才能注册 downgrade 脚本")
                for cm in _CUSTOM_MIGRATIONS[table_name]:
                    if cm.script_id == script_id:
                        if cm.downgrade is not None:
                            raise ValueError("同一脚本标识的 downgrade 脚本只能注册一次")
                        cm.downgrade = func
                        break
                else:
                    raise ValueError("未找到对应的 upgrade 脚本，无法注册 downgrade")
        return func

    return wrapper


# _load_state 和 _save_state 保持不变
def _load_state() -> dict[str, Any]:
    if not _STATE_FILE.exists():
        return {}
    try:
        with _STATE_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(data: dict[str, Any]):
    with _LOCK:
        _STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = _STATE_FILE.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        tmp.replace(_STATE_FILE)


def _get_table_structure(table: Table) -> dict[str, Any]:
    """将 SQLAlchemy Table 对象序列化为字典，用于后续哈希计算"""
    data = {
        "name": table.name,
        "metadata": repr(table.metadata),
        "columns": [repr(col) for col in sorted(table.columns, key=lambda c: c.name)],  # type: ignore
        "schema": ",".join([f"{k}={repr(getattr(table, k))}" for k in ["schema"]]),
    }

    # 序列化约束
    consts = list(table.constraints)
    consts.sort(key=lambda c: c.name if isinstance(c.name, str) else "")
    data["constraints"] = [repr(c) for c in consts]

    # 序列化索引
    indexes = list(table.indexes)
    indexes.sort(key=lambda i: i.name or "")
    data["indexes"] = [repr(i) for i in indexes]

    return data


def _compute_structure_hash(table: Table) -> str:
    """基于表结构计算稳定的哈希值"""
    structure = _get_table_structure(table)
    # 使用 sort_keys 确保 JSON 字符串的稳定性
    canonical_str = json.dumps(structure, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(canonical_str.encode("utf-8")).hexdigest()


def _model_revision(model: type[Base]) -> str:
    """
    生成模型的修订版本号。
    优先使用模型中自定义的 __revision__。
    否则，基于其 SQLAlchemy Table 结构计算哈希值作为版本号。
    """
    custom_rev = getattr(model, "__revision__", None)
    if custom_rev:
        return str(custom_rev)

    table = getattr(model, "__table__", None)
    if not isinstance(table, Table):
        raise ValueError(f"无法从模型 {model.__name__} 中获取有效的 Table 对象")

    return _compute_structure_hash(table)


def _include_tables_factory(target_tables: set[str]) -> Callable[[Any, str, str, bool, Any], bool]:
    def include(obj, name, type_, reflected, compare_to):  # type: ignore
        if type_ == "table":
            return name in target_tables
        table = getattr(getattr(obj, "table", None), "name", None)
        if table in target_tables:
            return True
        return False

    return include


def _execute_script(sync_conn, table: str, cm: CustomMigration, action: str) -> bool:
    with sync_conn.begin():
        mc = MigrationContext.configure(connection=sync_conn, opts={"target_metadata": Base.metadata})
        ops = Operations(mc)
        try:
            if action == "upgrade":
                cm.upgrade(ops, mc, table)
            else:
                if cm.downgrade is None:
                    logger.warning(f"脚本不支持 downgrade: {table} script={cm.script_id}")
                    return False
                cm.downgrade(ops, mc, table)
            return True
        except Exception as e:
            logger.exception(f"自定义脚本执行失败({action}): {table} script={cm.script_id}: {e}")
            return False


def _resolve_script_record(entry: dict, script_id: str):
    store = entry.setdefault("custom_scripts", {})
    raw = store.get(script_id)
    if raw is None:
        return None
    if isinstance(raw, str):
        store[script_id] = {"current": raw, "history": [raw]}
    return store[script_id]


def _plan_script(entry: dict, cm: CustomMigration) -> str | None:
    if cm.run_always:
        return "upgrade"
    rec = _resolve_script_record(entry, cm.script_id)
    if rec is None:
        return "upgrade"
    current = rec.get("current")
    history: list[str] = rec.get("history", [])
    if current == cm.script_rev:
        return None
    if cm.script_rev in history:
        try:
            idx_target = history.index(cm.script_rev)
            idx_current = history.index(current)
            if idx_target < idx_current:
                return "downgrade"
            return "upgrade"
        except ValueError:
            return "upgrade"
    return "upgrade"


def _plan_migrations(module: str, models: list[type[Base]], state: dict) -> dict[str, Any]:
    """
    分析模型，生成一个包含所有待办事项的“迁移计划”，不执行任何数据库操作。
    """
    model_info: dict[str, dict[str, Any]] = {}
    current_tables: set[str] = set()

    for m in models:
        table_obj = getattr(m, "__table__", None)
        if not isinstance(table_obj, Table):
            continue
        tablename = table_obj.name
        model_info[tablename] = {
            "model": m,
            "revision": _model_revision(m),
            "table_obj": table_obj
        }
        current_tables.add(tablename)

    rename_plan: list[dict[str, Any]] = []

    # 1. 识别当前模块中，状态文件有记录但代码中已不存在的表（潜在的旧表）
    obsolete_info = {
        t: info for t, info in state.items()
        if info.get("module") == module and t not in current_tables and "name" in info
    }
    # 2. 识别当前模块中，代码中存在但状态文件无记录的表（潜在的新表）
    new_tables = {t for t in current_tables if t not in state}

    # 3. 创建一个从 (模块, 模型名) 到旧表名的查找字典，用于快速匹配
    obsolete_map = {
        (info['module'], info['name']): t for t, info in obsolete_info.items()
    }

    # 4. 遍历新表，在旧表记录中寻找匹配项
    for new_name in new_tables:
        model = model_info[new_name]['model']
        model_name = model.__name__
        lookup_key = (module, model_name)

        if lookup_key in obsolete_map:
            # 找到了匹配项！确认为重命名操作
            old_name = obsolete_map.pop(lookup_key)  # 从map中移除，防止一个旧表匹配多个新表
            logger.debug(f"检测到表重命名 (基于模块与模型名): {old_name} -> {new_name}")
            rename_plan.append({
                "old_name": old_name,
                "new_name": new_name,
                "model_info": model_info[new_name]
            })

    # 5. 最终确定需要删除的表（未被匹配为重命名的旧表）
    obsolete_tables = set(obsolete_map.values())

    # 规划结构迁移（哪些表需要被alembic比对）
    target_tables: set[str] = set()
    for tablename, info in model_info.items():
        revision = info["revision"]

        # 检查是否是重命名后的新表
        is_renamed_new = any(r['new_name'] == tablename for r in rename_plan)
        if is_renamed_new:
            # 如果是，用它对应的旧表状态来判断版本是否变更
            old_name = next(r['old_name'] for r in rename_plan if r['new_name'] == tablename)
            entry = state.get(old_name, {})
        else:
            # 否则，正常使用当前表名获取状态
            entry = state.get(tablename, {})

        if entry.get("revision") != revision:
            target_tables.add(tablename)

    # 规划自定义脚本
    script_plan: dict[str, list[tuple[CustomMigration, str]]] = {}
    has_replacement: set[str] = set()
    for table, scripts in _CUSTOM_MIGRATIONS.items():
        if table not in current_tables:
            continue

        # 同样，为重命名的表加载旧状态以规划脚本
        is_renamed = any(r['new_name'] == table for r in rename_plan)
        if is_renamed:
            old_name = next(r['old_name'] for r in rename_plan if r['new_name'] == table)
            entry = state.get(old_name) or {}
        else:
            entry = state.get(table) or {}

        for cm in scripts:
            if cm.replace:
                has_replacement.add(table)
            action = _plan_script(entry, cm)
            if action:
                script_plan.setdefault(table, []).append((cm, action))

    # 最终计划
    return {
        "model_info": model_info,
        "target_tables": target_tables,
        "obsolete_tables": obsolete_tables,
        "script_plan": script_plan,
        "has_replacement": has_replacement,
        "rename_plan": rename_plan,
    }


def _update_state_for_model(state: dict, table_name: str, info: dict, module: str):
    """更新单个模型的 revision 状态"""
    entry = state.setdefault(table_name, {})
    rev_history: list[str] = entry.get("model_revision_history", [])
    cur_rev = info["revision"]

    if not rev_history or rev_history[-1] != cur_rev:
        if cur_rev not in rev_history:
            rev_history.append(cur_rev)

    entry["model_revision_history"] = rev_history
    entry.update({
        "revision": cur_rev,
        "name": f"{info['model'].__name__}",
        "module": module,  # 记录文件来源，用于后续删除判断
    })
    bind_key = info["table_obj"].info.get("bind_key", "")
    if bind_key:
        entry["bind_key"] = bind_key
    else:
        entry.pop("bind_key", None)


def _update_state_for_script(state: dict, table_name: str, cm: CustomMigration, action: str):
    """更新单个自定义脚本的执行状态"""
    entry = state.setdefault(table_name, {})
    store = entry.setdefault("custom_scripts", {})
    rec = store.get(cm.script_id)
    if rec is None:
        store[cm.script_id] = {"current": cm.script_rev, "history": [cm.script_rev]}
    else:
        hist: list[str] = rec.setdefault("history", [])
        if action == "upgrade":
            if cm.script_rev not in hist:
                hist.append(cm.script_rev)
        elif action == "downgrade":
            if cm.script_rev not in hist:
                hist.insert(0, cm.script_rev)
        rec["current"] = cm.script_rev


async def _execute_rename_and_update_state(rename_info: dict, service: SqlalchemyService, state: dict, module: str):
    old_name = rename_info["old_name"]
    new_name = rename_info["new_name"]
    model_info = rename_info["model_info"]
    bind_key = (state.get(old_name) or {}).get("bind_key", "")
    if bind_key not in service.engines:
        bind_key = ""
    engine = service.engines.get(bind_key) or service.engines.get("")
    if not engine:
        logger.error(f"无法找到用于重命名表 {old_name} 的引擎，跳过。")
        return
    try:
        async with engine.begin() as conn:
            def do_rename(sync_conn):
                mc = MigrationContext.configure(connection=sync_conn)
                ops = Operations(mc)
                ops.rename_table(old_name, new_name)
            await conn.run_sync(do_rename)
        logger.success(f"已重命名表{f'(bind={bind_key})' if bind_key else ''}: {old_name} -> {new_name}")
        if old_name in state:
            state[new_name] = state.pop(old_name)
        _update_state_for_model(state, new_name, model_info, module)
        _save_state(state)
    except Exception as e:
        logger.error(f"重命名表失败 {old_name} -> {new_name}: {e}")
        raise


async def _execute_migration_plan(plan: dict[str, Any], module: str, service: SqlalchemyService, state: dict):
    """
    根据迁移计划执行数据库操作，并在每一步成功后立即更新和保存状态。
    """
    if plan["rename_plan"]:
        for rename_info in plan["rename_plan"]:
            old_name = rename_info['old_name']
            new_name = rename_info['new_name']
            if old_name in state:
                state[new_name] = state[old_name].copy()
    for rename_info in plan["rename_plan"]:
        await _execute_rename_and_update_state(rename_info, service, state, module)

    # 按引擎分组
    tables_to_process = plan["target_tables"] | set(plan["script_plan"].keys())
    tables_by_engine: dict[str, set[str]] = {}
    for t in tables_to_process:
        table_obj = plan["model_info"].get(t, {}).get("table_obj")
        bind_key = table_obj.info.get("bind_key", "") if table_obj is not None else ""
        if bind_key not in service.engines:
            bind_key = ""  # fallback default
        tables_by_engine.setdefault(bind_key, set()).add(t)

    # 1. 执行自定义脚本 & 结构迁移
    for bind_key, tables in tables_by_engine.items():
        engine = service.engines.get(bind_key) or service.engines.get("")
        if engine is None:
            logger.error(f"未找到引擎: bind_key={bind_key}, 跳过表: {tables}")
            continue

        # 1a. 执行自定义脚本 (每个脚本成功后立即保存状态)
        async with engine.connect() as conn:
            for table in sorted(tables):
                for cm, action in plan["script_plan"].get(table, []):
                    ok = await conn.run_sync(_execute_script, table, cm, action)
                    if ok:
                        logger.info(f"自定义脚本{action}完成: {table} script={cm.script_id}->{cm.script_rev}")
                        _update_state_for_script(state, table, cm, action)
                        _save_state(state)  # 关键：立即保存状态

        # 1b. 执行自动结构迁移 (整个批次成功后保存状态)
        auto_tables = {t for t in tables if t in plan["target_tables"] and t not in plan["has_replacement"]}
        if auto_tables:
            async with engine.begin() as conn:
                def migrate(sync_conn):
                    mc = MigrationContext.configure(
                        connection=sync_conn,
                        opts={
                            "target_metadata": Base.metadata,
                            "include_object": _include_tables_factory(auto_tables),
                            "compare_type": True,
                            "compare_server_default": True,
                        },
                    )
                    script = autogen_api.produce_migrations(mc, Base.metadata)
                    if not script.upgrade_ops or script.upgrade_ops.is_empty():
                        return False

                    op_runner = Operations(mc)
                    upgrade_ops = script.upgrade_ops
                    applied = False
                    if sync_conn.dialect.name != "sqlite":
                        def apply_ops(ops_list: Iterable[Any]):
                            nonlocal applied
                            for _op in ops_list:
                                if isinstance(_op, alembic_ops.ModifyTableOps):
                                    apply_ops(_op.ops)
                                else:
                                    op_runner.invoke(_op)
                                    applied = True

                        apply_ops(upgrade_ops.ops)
                        return applied
                    BATCH_OP_TYPES = (DropConstraintOp, CreateUniqueConstraintOp, AddConstraintOp, AlterColumnOp, DropColumnOp, CreateForeignKeyOp)  # noqa: E501

                    def iter_ops(ops_list):
                        for _op in ops_list:
                            if isinstance(_op, alembic_ops.ModifyTableOps):
                                for sub in _op.ops:
                                    yield _op.table_name, sub
                            else:
                                tn = getattr(_op, "table_name", None) or getattr(getattr(_op, "table", None), "name", None)  # noqa: E501
                                yield tn, _op

                    all_ops = list(iter_ops(upgrade_ops.ops))
                    need_batch: dict[str, bool] = {}
                    for tn, op_ in all_ops:
                        if tn and isinstance(op_, BATCH_OP_TYPES):
                            need_batch[tn] = True
                    current_batch = None
                    batch_ctx = None
                    runner = op_runner

                    def close_batch():
                        nonlocal batch_ctx, current_batch, runner
                        if batch_ctx:
                            batch_ctx.__exit__(None, None, None)
                            batch_ctx = None
                            current_batch = None
                            runner = op_runner

                    for tn, op_ in all_ops:
                        if tn not in auto_tables:
                            continue
                        if need_batch.get(tn):
                            if current_batch != tn:
                                close_batch()
                                batch_ctx = op_runner.batch_alter_table(tn)
                                runner = batch_ctx.__enter__()
                                current_batch = tn
                        else:
                            if current_batch:
                                close_batch()
                        runner.invoke(op_)
                        applied = True
                    close_batch()
                    return applied

                changed = await conn.run_sync(migrate)
                if changed:
                    logger.success(f"已迁移表{f'(bind={bind_key})' if bind_key else ''}: {', '.join(sorted(auto_tables))}")  # noqa: E501
                    for t in auto_tables:
                        info = plan["model_info"][t]
                        _update_state_for_model(state, t, info, module)
                    _save_state(state)  # 关键：批次成功后保存状态

    # 2. 删除表 (每删除一个表就保存一次状态)
    if plan["obsolete_tables"]:
        # 按 bind_key 分组待删除的表，优先使用 lock 中记录的 bind_key
        obsolete_by_engine: dict[str, set[str]] = {}
        for t_name in plan["obsolete_tables"]:
            bind_key = (state.get(t_name, {}) or {}).get("bind_key", "")
            if bind_key not in service.engines:
                bind_key = ""
            obsolete_by_engine.setdefault(bind_key, set()).add(t_name)

        for bind_key, tables in obsolete_by_engine.items():
            engine = service.engines.get(bind_key) or service.engines.get("")
            if engine is None:
                logger.error(f"未找到引擎用于删表: bind_key={bind_key}, 跳过表: {tables}")
                continue

            meta = MetaData()
            # 反射目标引擎上存在的这些表
            async with engine.begin() as conn:
                await conn.run_sync(meta.reflect, only=list(tables))

            # 逐个删除（存在才删），成功后更新并保存状态
            for t_name in sorted(tables):
                if t_name in meta.tables:
                    try:
                        async with engine.begin() as conn:
                            await conn.run_sync(meta.tables[t_name].drop, checkfirst=True)
                        logger.success(f"已删除表{f'(bind={bind_key})' if bind_key else ''}: {t_name}")
                        state.pop(t_name, None)
                        _save_state(state)
                    except Exception as e:
                        logger.error(f"删除表失败{f'(bind={bind_key})' if bind_key else ''}: {t_name}: {e}")


async def run_migration_for(module: str, service: SqlalchemyService):
    """
    针对某个源码文件生成并执行迁移。
    重构后的主流程：规划 -> 执行 -> 增量式状态更新。
    """
    with _LOCK:
        if module not in _MODULE_MODELS:
            return
        models = list(_MODULE_MODELS[module].values())
        models.sort(key=lambda c: c.__name__)
    if not models:
        return

    state = _load_state()
    plan = _plan_migrations(module, models, state)

    try:
        await _execute_migration_plan(plan, module, service, state)
    except Exception as e:
        logger.exception(f"模块 {module} 的迁移过程发生未处理的异常: {e}")
    is_state_dirty = False
    for t, info in plan["model_info"].items():
        if state.get(t, {}).get("revision") != info["revision"]:
            _update_state_for_model(state, t, info, module)
            is_state_dirty = True
    if is_state_dirty:
        _save_state(state)
    return
