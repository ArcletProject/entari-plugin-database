import ast
import inspect
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

from arclet.entari.plugin.service import plugin_service
from graia.amnesia.builtins.sqla.model import Base
from graia.amnesia.builtins.sqla.service import SqlalchemyService
from arclet.entari.localdata import local_data
from sqlalchemy import MetaData

from .utils import logger

_STATE_FILE = local_data.get_data_file("database", "migrations_lock.json")
_FILE_MODELS: dict[str, set[type[Base]]] = {}
_LOCK = RLock()


@dataclass
class CustomMigration:
    script_id: str
    script_rev: str  # 目标脚本版本(按出现顺序作为时间线)
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


def _load_state() -> dict[str, Any]:
    if not _STATE_FILE.exists():
        return {}
    try:
        with _STATE_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(data: dict[str, Any]):
    _STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = _STATE_FILE.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
    tmp.replace(_STATE_FILE)


def _get_model_source(model: type[Base]) -> str:
    """提取单个模型源代码文本(尽量精确到类定义)"""
    try:
        if model.__module__ in plugin_service.plugins:
            qualname = model.__qualname__
            lines = inspect.getsourcelines(plugin_service.plugins[model.__module__].module)[0]
            source = "".join(lines)
            tree = ast.parse(source)
            class_finder = inspect._ClassFinder(qualname)  # type: ignore
            try:
                class_finder.visit(tree)
            except inspect.ClassFoundException as e:  # type: ignore
                line_number = e.args[0]
                src = "".join(inspect.getblock(lines[line_number:]))
            else:
                src = model.__name__
        else:
            src = inspect.getsource(model)
    except OSError:
        src = model.__name__
    return src.strip()


def _compute_model_hash(model: type[Base]) -> str:
    return hashlib.md5(_get_model_source(model).encode("utf-8")).hexdigest()


def _model_revision(model: type[Base], src_hash: str) -> str:
    # 模型可自定义 __migration_rev__ 强制触发
    custom = getattr(model, "__migration_rev__", None)
    if custom:
        return str(custom)
    return src_hash


def _include_tables_factory(target_tables: set[str]) -> Callable[[Any, str, str, bool, Any], bool]:
    def include(obj, name, type_, reflected, compare_to):  # type: ignore
        # 仅筛选需要的表以及相关的索引/约束/列
        if type_ == "table":
            return name in target_tables
        table = getattr(getattr(obj, "table", None), "name", None)
        if table in target_tables:
            return True
        return False
    return include


def _execute_script(sync_conn, table: str, cm: CustomMigration, action: str) -> bool:
    """单脚本独立事务执行 (upgrade / downgrade)。失败自动回滚。"""
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
    if isinstance(raw, str):  # 旧格式升级
        store[script_id] = {"current": raw, "history": [raw]}
    return store[script_id]


def _plan_script(entry: dict, cm: CustomMigration) -> str | None:
    # 返回 upgrade / downgrade / None
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
        # 历史中索引越小时间越早
        try:
            idx_target = history.index(cm.script_rev)
            idx_current = history.index(current)
            if idx_target < idx_current:
                return "downgrade"
            # idx_target > idx_current 理论上不会出现(因为 current 应为最后一条) 仍视为 upgrade
            return "upgrade"
        except ValueError:
            return "upgrade"
    # 新版本(未出现过)
    return "upgrade"


async def run_migration_for(file_path: str, service: SqlalchemyService):
    """针对某个源码文件(包含多个模型)生成并执行迁移。
    同一文件内一次性 autogen, 但每个模型(表)有独立 revision(md5)。
    支持用户注册的自定义迁移脚本, 优先级高于自动迁移。"""
    with _LOCK:
        if file_path not in _FILE_MODELS:
            return
        models = sorted(_FILE_MODELS[file_path], key=lambda c: c.__name__)
    if not models:
        return

    state = _load_state()

    # 收集模型信息
    model_info: dict[str, dict[str, Any]] = {}
    current_tables: set[str] = set()
    target_tables: set[str] = set()  # 需要结构比对/自动迁移(不考虑 replace)

    for m in models:
        tablename = getattr(m, "__tablename__", None) or getattr(getattr(m, "__table__", None), "name", None)
        if not tablename:
            continue
        src_hash = _compute_model_hash(m)
        revision = _model_revision(m, src_hash)
        entry = state.get(tablename)
        prev_rev = entry.get("revision") if entry else None
        if (not entry) or entry.get("revision") != revision:
            target_tables.add(tablename)
        table_obj = Base.metadata.tables.get(tablename)
        bind_key = ""
        if table_obj is not None:
            bind_key = table_obj.info.get("bind_key", "") or ""
            if bind_key not in service.engines:
                bind_key = ""  # fallback default
        model_info[tablename] = {
            "model": m,
            "src_hash": src_hash,
            "revision": revision,
            "prev_rev": prev_rev,
            "bind_key": bind_key,
        }
        current_tables.add(tablename)

    # 需要删除的表
    obsolete_tables: set[str] = {
        t for t, info in state.items() if info.get("file") == file_path and t not in current_tables
    }

    # 生成脚本执行计划: table -> list[(CustomMigration, action)]
    script_plan: dict[str, list[tuple[CustomMigration, str]]] = {}
    has_replacement: set[str] = set()
    for table, scripts in _CUSTOM_MIGRATIONS.items():
        if table not in current_tables:
            continue
        entry = state.get(table) or {}
        for cm in scripts:
            if cm.replace:
                has_replacement.add(table)
            action = _plan_script(entry, cm)
            if action:
                script_plan.setdefault(table, []).append((cm, action))

    extra_tables = set(script_plan.keys()) - target_tables
    if not target_tables and not obsolete_tables and not extra_tables:
        return

    tables_to_process = target_tables | extra_tables
    tables_by_engine: dict[str, set[str]] = {}
    for t in tables_to_process:
        bk = model_info.get(t, {}).get("bind_key", "")
        tables_by_engine.setdefault(bk, set()).add(t)

    # 记录成功脚本
    script_exec_success: dict[str, dict[str, bool]] = {}

    # 执行脚本 (独立事务) & 结构迁移
    for bind_key, tables in tables_by_engine.items():
        engine = service.engines.get(bind_key) or service.engines.get("")
        if engine is None:
            logger.error(f"未找到引擎: bind_key={bind_key}, 跳过表: {tables}")
            continue
        # 1. 自定义脚本
        async with engine.connect() as conn:
            for table in sorted(tables):
                for cm, action in script_plan.get(table, []):
                    def run_script(sync_conn, table=table, cm=cm, action=action):
                        return _execute_script(sync_conn, table, cm, action)
                    ok = await conn.run_sync(run_script)
                    script_exec_success.setdefault(table, {})[cm.script_id] = ok
                    if ok:
                        logger.info(f"自定义脚本{action}完成: {table} script={cm.script_id}->{cm.script_rev}")
        # 2. 自动结构迁移
        auto_tables = {t for t in tables if t in target_tables and t not in has_replacement}
        if not auto_tables:
            continue
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
                migration_script = autogen_api.produce_migrations(mc, Base.metadata)  # type: ignore
                upgrade_ops = migration_script.upgrade_ops
                if not upgrade_ops or upgrade_ops.is_empty():
                    return False
                op_runner = Operations(mc)
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
                BATCH_OP_TYPES = (
                    DropConstraintOp,
                    CreateUniqueConstraintOp,
                    AddConstraintOp,
                    AlterColumnOp,
                    DropColumnOp,
                    CreateForeignKeyOp,
                )
                def iter_ops(ops_list):
                    for _op in ops_list:
                        if isinstance(_op, alembic_ops.ModifyTableOps):
                            for sub in _op.ops:
                                yield _op.table_name, sub
                        else:
                            tn = getattr(_op, "table_name", None) or getattr(getattr(_op, "table", None), "name", None)
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
                logger.success(f"已迁移表(bind={bind_key}): {', '.join(sorted(auto_tables))}")

    # 删除表
    if obsolete_tables:
        default_engine = service.engines.get("")
        if default_engine:
            async with default_engine.begin() as conn:
                def drop(sync_conn):
                    meta = MetaData()
                    try:
                        meta.reflect(sync_conn, only=list(obsolete_tables))
                    except Exception:
                        return False
                    dropped_any = False
                    for name in list(meta.tables):
                        try:
                            meta.tables[name].drop(sync_conn, checkfirst=True)
                            dropped_any = True
                        except Exception:
                            pass
                    return dropped_any
                dropped = await conn.run_sync(drop)
                if dropped:
                    logger.success(f"已删除表: {', '.join(sorted(obsolete_tables))}")
        for t in obsolete_tables:
            state.pop(t, None)

    # 更新状态写入
    for t, info in model_info.items():
        entry = state.get(t) or {}
        # 模型 revision 历史
        rev_history: list[str] = entry.get("model_revision_history") or []
        cur_rev = info["revision"]
        if not rev_history:
            rev_history = [cur_rev]
        else:
            if rev_history[-1] != cur_rev:
                # 若回退到旧版本(存在于历史中间), 不追加; 若新版本, 追加
                if cur_rev not in rev_history:
                    rev_history.append(cur_rev)
        entry["model_revision_history"] = rev_history
        entry.update({
            "hash": info["src_hash"],
            "path": f"{info['model'].__module__}.{info['model'].__name__}",
            "revision": cur_rev,
            "custom_scripts": entry.get("custom_scripts", {}),
        })
        if info["bind_key"]:
            entry["bind_key"] = info["bind_key"]
        # 脚本执行结果
        for cm, action in (script_plan.get(t, []) or []):
            if not script_exec_success.get(t, {}).get(cm.script_id):
                continue
            store = entry.setdefault("custom_scripts", {})
            rec = store.get(cm.script_id)
            if rec is None:  # 兼容旧格式
                store[cm.script_id] = {"current": cm.script_rev, "history": [cm.script_rev]}
            else:
                hist: list[str] = rec.setdefault("history", [])
                if action == "upgrade":
                    if cm.script_rev not in hist:
                        hist.append(cm.script_rev)
                    rec["current"] = cm.script_rev
                elif action == "downgrade":
                    # 目标在历史内, 只更新 current; 若意外未在历史, 前插
                    if cm.script_rev not in hist:
                        hist.insert(0, cm.script_rev)
                    rec["current"] = cm.script_rev
        state[t] = entry

    _save_state(state)
