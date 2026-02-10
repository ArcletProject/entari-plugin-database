"""数据库迁移模块，支持自动结构迁移和自定义迁移脚本。"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from threading import RLock
from typing import TYPE_CHECKING, Any, Literal, TypeAlias

from alembic.autogenerate import api as autogen_api
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.operations import ops as alembic_ops
from alembic.operations.ops import (
    AddConstraintOp,
    AlterColumnOp,
    CreateForeignKeyOp,
    CreateUniqueConstraintOp,
    DropColumnOp,
    DropConstraintOp,
)
from arclet.entari.localdata import local_data
from graia.amnesia.builtins.sqla.model import Base
from graia.amnesia.builtins.sqla.service import SqlalchemyService
from graia.amnesia.builtins.utils import get_subclasses
from sqlalchemy import (
    CheckConstraint,
    ForeignKeyConstraint,
    MetaData,
    PrimaryKeyConstraint,
    UniqueConstraint,
)
from sqlalchemy.schema import Table

from .utils import logger

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

# ============= 类型别名 =============
MigrationFunc: TypeAlias = Callable[[Operations, MigrationContext, str], None]
StateDict: TypeAlias = dict[str, Any]
ScriptAction: TypeAlias = Literal["upgrade", "downgrade"]

# ============= 常量定义 =============
_STATE_FILE = local_data.get_data_file("database", "migrations_lock.json")
_LOCK = RLock()
_DEFAULT_BIND_KEY = ""

# SQLite 需要使用 batch 模式的操作类型
_BATCH_OP_TYPES: tuple[type, ...] = (
    DropConstraintOp,
    CreateUniqueConstraintOp,
    AddConstraintOp,
    AlterColumnOp,
    DropColumnOp,
    CreateForeignKeyOp,
)


@dataclass(slots=True)
class CustomMigration:
    """自定义迁移脚本的配置类。"""

    script_id: str
    script_rev: str
    replace: bool
    run_always: bool
    upgrade: MigrationFunc
    downgrade: MigrationFunc | None = field(default=None)


# 全局注册表：表名 -> 自定义迁移脚本列表
_CUSTOM_MIGRATIONS: dict[str, list[CustomMigration]] = {}


def _resolve_table_name(model_or_table: str | type[Base]) -> str:
    """从模型或字符串解析表名。"""
    if isinstance(model_or_table, str):
        return model_or_table
    table_name = getattr(model_or_table, "__tablename__", None)
    if not table_name:
        raise ValueError("无法确定表名, 请传入 ORM 模型或表名")
    return table_name


def register_custom_migration(
    model_or_table: str | type[Base],
    action_type: ScriptAction = "upgrade",
    *,
    script_id: str | None = None,
    script_rev: str = "1",
    replace: bool = True,
    run_always: bool = False,
) -> Callable[[MigrationFunc], MigrationFunc]:
    """
    注册自定义迁移脚本。

    Args:
        model_or_table: 目标 ORM 模型或表名
        action_type: 脚本类型，默认为 "upgrade"。
            如果需要注册降级脚本，请传入 "downgrade" 并提供 downgrade 函数。
        script_id: 目标脚本标识，若需要 downgrade 则连同 upgrade 一起传入相同标识
        script_rev: 目标脚本版本，变化触发 upgrade/downgrade
        replace: True 时跳过该表自动结构迁移
        run_always: 每次都会执行 upgrade（仍记录版本）

    Returns:
        装饰器函数

    Raises:
        ValueError: 当表名无法确定或注册配置无效时
    """
    table_name = _resolve_table_name(model_or_table)

    def wrapper(func: MigrationFunc) -> MigrationFunc:
        resolved_id = script_id or func.__name__ or "anonymous"

        with _LOCK:
            if action_type == "upgrade":
                _CUSTOM_MIGRATIONS.setdefault(table_name, []).append(
                    CustomMigration(
                        script_id=resolved_id,
                        script_rev=str(script_rev),
                        replace=replace,
                        run_always=run_always,
                        upgrade=func,
                    )
                )
            else:
                _register_downgrade(table_name, resolved_id, func)
        return func

    return wrapper


def _register_downgrade(table_name: str, script_id: str, func: MigrationFunc) -> None:
    """注册 downgrade 脚本到已存在的 upgrade 脚本。"""
    if not script_id:
        raise ValueError("注册 downgrade 脚本必须提供 script_id")

    migrations = _CUSTOM_MIGRATIONS.get(table_name)
    if not migrations:
        raise ValueError("必须先注册 upgrade 脚本后才能注册 downgrade 脚本")

    for cm in migrations:
        if cm.script_id == script_id:
            if cm.downgrade is not None:
                raise ValueError("同一脚本标识的 downgrade 脚本只能注册一次")
            cm.downgrade = func
            return

    raise ValueError("未找到对应的 upgrade 脚本，无法注册 downgrade")


# ============= 状态持久化 =============


def _load_state() -> StateDict:
    """加载迁移状态文件。"""
    if not _STATE_FILE.exists():
        return {}
    try:
        with _STATE_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def _save_state(data: StateDict) -> None:
    """原子性保存迁移状态文件。"""
    with _LOCK:
        _STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = _STATE_FILE.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        tmp.replace(_STATE_FILE)


# ============= 表结构分析 =============


def _serialize_constraint(const: Any) -> dict[str, Any]:
    """序列化单个约束为字典。"""
    const_info: dict[str, Any] = {
        "type": const.__class__.__name__,
        "name": const.name,
    }

    if isinstance(const, (PrimaryKeyConstraint, UniqueConstraint)):
        const_info["columns"] = sorted(c.name for c in const.columns)
    elif isinstance(const, ForeignKeyConstraint):
        const_info["columns"] = sorted(c.name for c in const.columns)
        const_info["target"] = const.elements[0].target_fullname
        const_info["ondelete"] = const.ondelete
        const_info["onupdate"] = const.onupdate
    elif isinstance(const, CheckConstraint):
        const_info["sqltext"] = str(const.sqltext)

    return const_info


def _get_table_structure(table: Table) -> dict[str, Any]:
    """将 SQLAlchemy Table 对象序列化为字典，用于后续哈希计算。"""
    # 按名称排序约束和索引以确保稳定性
    sorted_constraints = sorted(
        table.constraints,
        key=lambda c: c.name if isinstance(c.name, str) else "",
    )
    sorted_indexes = sorted(table.indexes, key=lambda i: i.name or "")

    return {
        "name": table.name,
        "metadata": repr(table.metadata),
        "columns": [repr(col) for col in sorted(table.columns, key=lambda c: c.name)],
        "schema": f"schema={table.schema!r}",
        "constraints": [_serialize_constraint(c) for c in sorted_constraints],
        "indexes": [repr(i) for i in sorted_indexes],
    }


def _compute_structure_hash(table: Table) -> str:
    """基于表结构计算稳定的 MD5 哈希值。"""
    structure = _get_table_structure(table)
    canonical_str = json.dumps(structure, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(canonical_str.encode("utf-8")).hexdigest()


# ============= 模型解析 =============


def _resolve_model_table(model: type[Base]) -> Table | None:
    """解析模型对应的 Table 对象。"""
    # 优先使用模型上的 __table__
    table_obj = getattr(model, "__table__", None)
    if isinstance(table_obj, Table):
        return table_obj

    # 回退到 metadata.tables 查找
    tablename = getattr(model, "__tablename__", None)
    return Base.metadata.tables.get(tablename) if tablename else None


def _model_revision(model: type[Base], table: Table) -> str:
    """
    生成模型的修订版本号。

    优先使用模型中自定义的 __revision__，否则基于表结构计算哈希值。
    """
    custom_rev = getattr(model, "__revision__", None)
    return str(custom_rev) if custom_rev else _compute_structure_hash(table)


# ============= Alembic 迁移辅助 =============


def _include_tables_factory(target_tables: set[str]) -> Callable[[Any, str, str, bool, Any], bool]:
    """创建 Alembic 对象过滤函数，仅包含目标表。"""

    def include(obj: Any, name: str, type_: str, reflected: bool, compare_to: Any) -> bool:
        if type_ == "table":
            return name in target_tables
        # 对于非表对象，检查其所属表
        table_name = getattr(getattr(obj, "table", None), "name", None)
        return table_name in target_tables

    return include


def _execute_script(
    sync_conn: Connection,
    table: str,
    cm: CustomMigration,
    action: ScriptAction,
) -> bool:
    """执行单个自定义迁移脚本。"""
    with sync_conn.begin():
        mc = MigrationContext.configure(
            connection=sync_conn,
            opts={"target_metadata": Base.metadata},
        )
        ops = Operations(mc)
        try:
            if action == "upgrade":
                cm.upgrade(ops, mc, table)
            elif cm.downgrade is not None:
                cm.downgrade(ops, mc, table)
            else:
                logger.warning(f"脚本不支持 downgrade: {table} script={cm.script_id}")
                return False
            return True
        except Exception as e:
            logger.exception(f"自定义脚本执行失败({action}): {table} script={cm.script_id}: {e}")
            return False


def _resolve_script_record(entry: StateDict, script_id: str) -> dict[str, Any] | None:
    """解析并规范化脚本状态记录。"""
    store = entry.setdefault("custom_scripts", {})
    raw = store.get(script_id)
    if raw is None:
        return None
    # 兼容旧格式：将字符串转换为新格式
    if isinstance(raw, str):
        store[script_id] = {"current": raw, "history": [raw]}
    return store[script_id]


def _plan_script(entry: StateDict, cm: CustomMigration) -> ScriptAction | None:
    """规划单个脚本的执行动作。"""
    if cm.run_always:
        return "upgrade"

    rec = _resolve_script_record(entry, cm.script_id)
    if rec is None:
        return "upgrade"

    current = rec.get("current")
    if current == cm.script_rev:
        return None

    history: list[str] = rec.get("history", [])
    if cm.script_rev not in history:
        return "upgrade"

    # 判断是升级还是降级
    try:
        idx_target = history.index(cm.script_rev)
        idx_current = history.index(current) if current else -1
        return "downgrade" if idx_target < idx_current else "upgrade"
    except ValueError:
        return "upgrade"


# ============= 迁移计划 =============


@dataclass(slots=True)
class MigrationPlan:
    """迁移计划，包含所有待执行的迁移操作。"""

    model_info: dict[str, dict[str, Any]]
    target_tables: set[str]
    obsolete_tables: set[str]
    script_plan: dict[str, list[tuple[CustomMigration, ScriptAction]]]
    has_replacement: set[str]
    rename_plan: list[dict[str, Any]]


def _find_renamed_old_name(rename_plan: list[dict[str, Any]], new_name: str) -> str | None:
    """查找重命名计划中新表名对应的旧表名。"""
    for r in rename_plan:
        if r["new_name"] == new_name:
            return r["old_name"]
    return None


def _plan_migrations(module: str, models: list[type[Base]], state: StateDict) -> MigrationPlan:
    """分析模型，生成迁移计划，不执行任何数据库操作。"""
    model_info: dict[str, dict[str, Any]] = {}
    current_tables: set[str] = set()

    for m in models:
        table_obj = _resolve_model_table(m)
        if table_obj is None:
            logger.warning(f"[Migration] 跳过模型 {m.__module__}:{m.__name__}: 未能找到已定义的 Table。")
            continue

        tablename = table_obj.name
        model_info[tablename] = {
            "model": m,
            "revision": _model_revision(m, table_obj),
            "table_obj": table_obj,
        }
        current_tables.add(tablename)

    # 检测表重命名
    rename_plan = _detect_table_renames(module, model_info, current_tables, state)
    renamed_old_names = {r["old_name"] for r in rename_plan}

    # 计算废弃表（排除已被识别为重命名的）
    obsolete_tables = {
        t
        for t, info in state.items()
        if info.get("module") == module and t not in current_tables and "name" in info and t not in renamed_old_names
    }

    # 规划结构迁移
    target_tables = _plan_structure_migrations(model_info, rename_plan, state)

    # 规划自定义脚本
    script_plan, has_replacement = _plan_custom_scripts(current_tables, rename_plan, state)

    return MigrationPlan(
        model_info=model_info,
        target_tables=target_tables,
        obsolete_tables=obsolete_tables,
        script_plan=script_plan,
        has_replacement=has_replacement,
        rename_plan=rename_plan,
    )


def _detect_table_renames(
    module: str,
    model_info: dict[str, dict[str, Any]],
    current_tables: set[str],
    state: StateDict,
) -> list[dict[str, Any]]:
    """检测表重命名操作。"""
    # 识别旧表：状态有记录但代码中不存在
    obsolete_info = {
        t: info
        for t, info in state.items()
        if info.get("module") == module and t not in current_tables and "name" in info
    }

    # 识别新表：代码中存在但状态无记录
    new_tables = current_tables - state.keys()

    # (模块, 模型名) -> 旧表名 查找表
    obsolete_map: dict[tuple[str, str], str] = {(info["module"], info["name"]): t for t, info in obsolete_info.items()}

    rename_plan: list[dict[str, Any]] = []
    for new_name in new_tables:
        model = model_info[new_name]["model"]
        lookup_key = (module, model.__name__)

        if lookup_key in obsolete_map:
            old_name = obsolete_map.pop(lookup_key)
            logger.debug(f"检测到表重命名: {old_name} -> {new_name}")
            rename_plan.append(
                {
                    "old_name": old_name,
                    "new_name": new_name,
                    "model_info": model_info[new_name],
                }
            )

    return rename_plan


def _plan_structure_migrations(
    model_info: dict[str, dict[str, Any]],
    rename_plan: list[dict[str, Any]],
    state: StateDict,
) -> set[str]:
    """规划需要结构迁移的表。"""
    target_tables: set[str] = set()

    for tablename, info in model_info.items():
        # 获取对应的状态记录
        old_name = _find_renamed_old_name(rename_plan, tablename)
        entry = state.get(old_name or tablename, {})

        if entry.get("revision") != info["revision"]:
            target_tables.add(tablename)

    return target_tables


def _plan_custom_scripts(
    current_tables: set[str],
    rename_plan: list[dict[str, Any]],
    state: StateDict,
) -> tuple[dict[str, list[tuple[CustomMigration, ScriptAction]]], set[str]]:
    """规划自定义脚本执行。"""
    script_plan: dict[str, list[tuple[CustomMigration, ScriptAction]]] = {}
    has_replacement: set[str] = set()

    for table, scripts in _CUSTOM_MIGRATIONS.items():
        if table not in current_tables:
            continue

        # 获取对应的状态记录
        old_name = _find_renamed_old_name(rename_plan, table)
        entry = state.get(old_name or table) or {}

        for cm in scripts:
            if cm.replace:
                has_replacement.add(table)
            action = _plan_script(entry, cm)
            if action:
                script_plan.setdefault(table, []).append((cm, action))

    return script_plan, has_replacement


# ============= 状态更新 =============


def _update_state_for_model(
    state: StateDict,
    table_name: str,
    info: dict[str, Any],
    module: str,
) -> None:
    """更新单个模型的 revision 状态。"""
    entry = state.setdefault(table_name, {})
    rev_history: list[str] = entry.get("model_revision_history", [])
    cur_rev = info["revision"]

    # 追加新版本到历史记录
    if cur_rev not in rev_history:
        rev_history.append(cur_rev)

    entry["model_revision_history"] = rev_history
    entry["revision"] = cur_rev
    entry["name"] = info["model"].__name__
    entry["module"] = module

    # 处理 bind_key
    bind_key = info["table_obj"].info.get("bind_key", "")
    if bind_key:
        entry["bind_key"] = bind_key
    else:
        entry.pop("bind_key", None)


def _update_state_for_script(
    state: StateDict,
    table_name: str,
    cm: CustomMigration,
    action: ScriptAction,
) -> None:
    """更新单个自定义脚本的执行状态。"""
    entry = state.setdefault(table_name, {})
    store = entry.setdefault("custom_scripts", {})
    rec = store.get(cm.script_id)

    if rec is None:
        store[cm.script_id] = {"current": cm.script_rev, "history": [cm.script_rev]}
    else:
        hist: list[str] = rec.setdefault("history", [])
        if cm.script_rev not in hist:
            if action == "upgrade":
                hist.append(cm.script_rev)
            else:
                hist.insert(0, cm.script_rev)
        rec["current"] = cm.script_rev


# ============= 引擎解析辅助 =============


def _resolve_engine(service: SqlalchemyService, bind_key: str):
    """解析并获取数据库引擎。"""
    effective_key = bind_key if bind_key in service.engines else _DEFAULT_BIND_KEY
    return service.engines.get(effective_key) or service.engines.get(_DEFAULT_BIND_KEY)


def _format_bind_suffix(bind_key: str) -> str:
    """格式化 bind_key 日志后缀。"""
    return f"(bind={bind_key})" if bind_key else ""


# ============= 迁移执行 =============


async def _execute_rename_and_update_state(
    rename_info: dict[str, Any],
    service: SqlalchemyService,
    state: StateDict,
    module: str,
) -> None:
    """执行表重命名并更新状态。"""
    old_name = rename_info["old_name"]
    new_name = rename_info["new_name"]
    model_info = rename_info["model_info"]

    bind_key = (state.get(old_name) or {}).get("bind_key", _DEFAULT_BIND_KEY)
    engine = _resolve_engine(service, bind_key)
    if not engine:
        logger.error(f"无法找到用于重命名表 {old_name} 的引擎，跳过")
        return

    try:
        async with engine.begin() as conn:

            def do_rename(sync_conn: Connection) -> None:
                mc = MigrationContext.configure(connection=sync_conn)
                Operations(mc).rename_table(old_name, new_name)

            await conn.run_sync(do_rename)

        suffix = _format_bind_suffix(bind_key if bind_key in service.engines else "")
        logger.success(f"已重命名表{suffix}: {old_name} -> {new_name}")

        if old_name in state:
            state[new_name] = state.pop(old_name)
        _update_state_for_model(state, new_name, model_info, module)
        _save_state(state)
    except Exception as e:
        logger.error(f"重命名表失败 {old_name} -> {new_name}: {e}")
        raise


async def _execute_migration_plan(
    plan: MigrationPlan,
    module: str,
    service: SqlalchemyService,
    state: StateDict,
) -> None:
    """
    根据迁移计划执行数据库操作。

    每一步成功后立即更新并保存状态，确保崩溃恢复安全。
    """
    # 预处理重命名状态
    for rename_info in plan.rename_plan:
        old_name = rename_info["old_name"]
        new_name = rename_info["new_name"]
        if old_name in state:
            state[new_name] = state[old_name].copy()

    # 执行重命名
    for rename_info in plan.rename_plan:
        await _execute_rename_and_update_state(rename_info, service, state, module)

    # 按引擎分组处理
    await _execute_scripts_and_migrations(plan, module, service, state)

    # 删除废弃表
    if plan.obsolete_tables:
        await _execute_table_drops(plan.obsolete_tables, service, state)


def _group_tables_by_engine(
    plan: MigrationPlan,
    service: SqlalchemyService,
) -> dict[str, set[str]]:
    """按数据库引擎分组待处理的表。"""
    tables_to_process = plan.target_tables | set(plan.script_plan.keys())
    tables_by_engine: dict[str, set[str]] = {}

    for t in tables_to_process:
        table_obj = plan.model_info.get(t, {}).get("table_obj")
        bind_key = table_obj.info.get("bind_key", "") if table_obj is not None else _DEFAULT_BIND_KEY
        effective_key = bind_key if bind_key in service.engines else _DEFAULT_BIND_KEY
        tables_by_engine.setdefault(effective_key, set()).add(t)

    return tables_by_engine


async def _execute_scripts_and_migrations(
    plan: MigrationPlan,
    module: str,
    service: SqlalchemyService,
    state: StateDict,
) -> None:
    """执行自定义脚本和自动结构迁移。"""
    tables_by_engine = _group_tables_by_engine(plan, service)

    for bind_key, tables in tables_by_engine.items():
        engine = _resolve_engine(service, bind_key)
        if engine is None:
            logger.error(f"未找到引擎: bind_key={bind_key}, 跳过表: {tables}")
            continue

        # 执行自定义脚本
        async with engine.connect() as conn:
            for table in sorted(tables):
                for cm, action in plan.script_plan.get(table, []):
                    ok = await conn.run_sync(_execute_script, table, cm, action)
                    if ok:
                        logger.info(f"自定义脚本{action}完成: {table} script={cm.script_id}->{cm.script_rev}")
                        _update_state_for_script(state, table, cm, action)
                        _save_state(state)

        # 执行自动结构迁移
        auto_tables = {t for t in tables if t in plan.target_tables and t not in plan.has_replacement}
        if auto_tables:
            await _execute_auto_migration(engine, auto_tables, plan, module, bind_key, state)


async def _execute_auto_migration(
    engine,
    auto_tables: set[str],
    plan: MigrationPlan,
    module: str,
    bind_key: str,
    state: StateDict,
) -> None:
    """执行 Alembic 自动结构迁移。"""

    def migrate(sync_conn: Connection) -> bool:
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

        # 非 SQLite 直接应用
        if sync_conn.dialect.name != "sqlite":
            return _apply_ops_direct(op_runner, upgrade_ops.ops)

        # SQLite 使用 batch 模式
        return _apply_ops_sqlite_batch(op_runner, upgrade_ops.ops, auto_tables)

    async with engine.begin() as conn:
        changed = await conn.run_sync(migrate)
        if changed:
            suffix = _format_bind_suffix(bind_key)
            logger.success(f"已迁移表{suffix}: {', '.join(sorted(auto_tables))}")
            for t in auto_tables:
                _update_state_for_model(state, t, plan.model_info[t], module)
            _save_state(state)


def _apply_ops_direct(op_runner: Operations, ops_list: Iterable[Any]) -> bool:
    """直接应用迁移操作（非 SQLite）。"""
    applied = False

    def apply(ops: Iterable[Any]) -> None:
        nonlocal applied
        for op in ops:
            if isinstance(op, alembic_ops.ModifyTableOps):
                apply(op.ops)
            else:
                op_runner.invoke(op)
                applied = True

    apply(ops_list)
    return applied


def _apply_ops_sqlite_batch(
    op_runner: Operations,
    ops_list: Iterable[Any],
    target_tables: set[str],
) -> bool:
    """使用 batch 模式应用迁移操作（SQLite）。"""

    def iter_ops(ops: Iterable[Any]):
        for op in ops:
            if isinstance(op, alembic_ops.ModifyTableOps):
                for sub in op.ops:
                    yield op.table_name, sub
            else:
                tn = getattr(op, "table_name", None) or getattr(getattr(op, "table", None), "name", None)
                yield tn, op

    all_ops = list(iter_ops(ops_list))

    # 确定哪些表需要 batch 模式
    need_batch: dict[str, bool] = {}
    for tn, op in all_ops:
        if tn and isinstance(op, _BATCH_OP_TYPES):
            need_batch[tn] = True

    applied = False
    current_batch: str | None = None
    batch_ctx = None
    runner = op_runner

    def close_batch() -> None:
        nonlocal batch_ctx, current_batch, runner
        if batch_ctx:
            batch_ctx.__exit__(None, None, None)
            batch_ctx = None
            current_batch = None
            runner = op_runner

    for tn, op in all_ops:
        if tn not in target_tables:
            continue

        if need_batch.get(tn):
            if current_batch != tn:
                close_batch()
                batch_ctx = op_runner.batch_alter_table(tn)
                runner = batch_ctx.__enter__()
                current_batch = tn
        elif current_batch:
            close_batch()

        runner.invoke(op)
        applied = True

    close_batch()
    return applied


async def _execute_table_drops(
    obsolete_tables: set[str],
    service: SqlalchemyService,
    state: StateDict,
) -> None:
    """删除废弃的表。"""
    # 按 bind_key 分组
    obsolete_by_engine: dict[str, set[str]] = {}
    for t_name in obsolete_tables:
        bind_key = (state.get(t_name) or {}).get("bind_key", _DEFAULT_BIND_KEY)
        effective_key = bind_key if bind_key in service.engines else _DEFAULT_BIND_KEY
        obsolete_by_engine.setdefault(effective_key, set()).add(t_name)

    for bind_key, tables in obsolete_by_engine.items():
        engine = _resolve_engine(service, bind_key)
        if engine is None:
            logger.error(f"未找到引擎用于删表: bind_key={bind_key}, 跳过表: {tables}")
            continue

        # 反射表结构
        meta = MetaData()
        async with engine.begin() as conn:
            await conn.run_sync(meta.reflect, only=list(tables))

        # 逐个删除
        suffix = _format_bind_suffix(bind_key)
        for t_name in sorted(tables):
            if t_name not in meta.tables:
                continue

            try:
                async with engine.begin() as conn:
                    await conn.run_sync(meta.tables[t_name].drop, checkfirst=True)
                logger.success(f"已删除表{suffix}: {t_name}")
                state.pop(t_name, None)
                _save_state(state)
            except Exception as e:
                logger.error(f"删除表失败{suffix}: {t_name}: {e}")


async def run_migration(service: SqlalchemyService) -> None:
    """
    对所有模型生成并执行迁移。

    主流程：规划 -> 执行 -> 增量式状态更新。
    """
    all_models = list(get_subclasses(Base))

    # 按模块分组
    grouped_models: dict[str, list[type[Base]]] = {}
    for m in all_models:
        grouped_models.setdefault(m.__module__, []).append(m)

    state = _load_state()

    for module, models in grouped_models.items():
        plan = _plan_migrations(module, models, state)

        try:
            await _execute_migration_plan(plan, module, service, state)
        except Exception as e:
            logger.exception(f"模块 {module} 的迁移过程发生未处理的异常: {e}")

        # 确保所有模型状态都已更新
        is_state_dirty = False
        for t, info in plan.model_info.items():
            if state.get(t, {}).get("revision") != info["revision"]:
                _update_state_for_model(state, t, info, module)
                is_state_dirty = True

        if is_state_dirty:
            _save_state(state)
