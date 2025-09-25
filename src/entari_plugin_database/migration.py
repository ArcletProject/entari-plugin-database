import ast
import inspect
import json
import hashlib
from collections.abc import Callable
from threading import RLock
from typing import Any

from alembic.migration import MigrationContext
from alembic.autogenerate import api as autogen_api
from alembic.operations import Operations
from alembic.operations import ops as alembic_ops  # 新增: 用于识别 ModifyTableOps
from alembic.operations.ops import (
    DropConstraintOp,
    CreateUniqueConstraintOp,
    AddConstraintOp,
    AlterColumnOp,
    DropColumnOp,
)

from arclet.entari.plugin.service import plugin_service
from graia.amnesia.builtins.sqla.model import Base
from graia.amnesia.builtins.sqla.service import SqlalchemyService
from arclet.entari.localdata import local_data
from sqlalchemy import MetaData

from .utils import logger

# 状态文件: 记录每个表最后一次使用的文件级哈希(revision) 与 文件路径
_STATE_FILE = local_data.get_data_file("database", "migrations_lock.json")

# 内存缓存: 同一源码文件内的模型集合
_FILE_MODELS: dict[str, set[type[Base]]] = {}
_LOCK = RLock()


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


def _model_sources(models: list[type[Base]]) -> str:
    pieces: list[str] = []
    for m in models:
        try:
            if m.__module__ in plugin_service.plugins:
                qualname = m.__qualname__
                lines = inspect.getsourcelines(plugin_service.plugins[m.__module__].module)[0]
                source = "".join(lines)
                tree = ast.parse(source)
                class_finder = inspect._ClassFinder(qualname)  # type: ignore
                try:
                    class_finder.visit(tree)
                except inspect.ClassFoundException as e:
                    line_number = e.args[0]
                    src = "".join(inspect.getblock(lines[line_number:]))
                else:
                    src = m.__name__
            else:
                src = inspect.getsource(m)
        except OSError:
            src = m.__name__
        pieces.append(src.strip())
    return "\n\n".join(pieces)


def _compute_file_hash(models: list[type[Base]]) -> str:
    content = _model_sources(models)
    return hashlib.md5(content.encode("utf-8")).hexdigest()


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


async def run_migration_for(file_path: str, service: SqlalchemyService):
    """针对某个源码文件(包含多个模型)生成并执行迁移"""
    with _LOCK:
        if file_path not in _FILE_MODELS:
            return
        models = sorted(_FILE_MODELS[file_path], key=lambda c: c.__name__)
    if not models:
        return

    # 计算文件级哈希(作为 revision id)
    revision_hash = _compute_file_hash(models)
    state = _load_state()

    current_tables: set[str] = set()
    # 需要检查的新增/修改表
    target_tables: set[str] = set()
    for m in models:
        tablename = getattr(m, "__tablename__", None)
        if not tablename and hasattr(m, "__table__"):
            tablename = getattr(m.__table__, "name", None)
        if not tablename:
            continue
        current_tables.add(tablename)
        entry = state.get(tablename)
        if (not entry) or entry.get("hash") != revision_hash:
            target_tables.add(tablename)

    # 检测需删除(从源码文件中消失)的表: state 中 file=该文件且不在 current_tables
    obsolete_tables: set[str] = {
        t for t, info in state.items() if info.get("file") == file_path and t not in current_tables
    }

    if not target_tables and not obsolete_tables:
        return  # 无变化

    # 先对新增/修改的表执行差异迁移
    if target_tables:
        for bind_key, engine in service.engines.items():
            async with engine.begin() as conn:
                def migrate(sync_conn):
                    include = _include_tables_factory(target_tables)
                    mc = MigrationContext.configure(
                        connection=sync_conn,
                        opts={
                            "target_metadata": Base.metadata,
                            "include_object": include,
                            "compare_type": True,
                            "compare_server_default": True,
                        },
                    )
                    migration_script = autogen_api.produce_migrations(mc, Base.metadata)  # type: ignore
                    upgrade_ops = migration_script.upgrade_ops
                    if not upgrade_ops or upgrade_ops.is_empty():
                        return False
                    op_runner = Operations(mc)

                    dialect_name = sync_conn.dialect.name
                    applied = False
                    if dialect_name != "sqlite":
                        # 非 SQLite: 直接递归执行
                        def apply_ops(ops_list):
                            nonlocal applied
                            for _op in ops_list:
                                if isinstance(_op, alembic_ops.ModifyTableOps):
                                    apply_ops(_op.ops)
                                else:
                                    op_runner.invoke(_op)
                                    applied = True

                        apply_ops(upgrade_ops.ops)
                        return applied

                    # 判定是否需要 batch (SQLite 对以下操作需要 copy & move)
                    BATCH_OP_TYPES = (DropConstraintOp, CreateUniqueConstraintOp, AddConstraintOp, AlterColumnOp, DropColumnOp)  # noqa: E501

                    def iter_table_ops(ops_list):
                        for _op in ops_list:
                            if isinstance(_op, alembic_ops.ModifyTableOps):
                                for sub in _op.ops:
                                    yield _op.table_name, sub
                            else:
                                tn = getattr(_op, "table_name", None) or getattr(getattr(_op, "table", None), "name", None)  # noqa: E501
                                yield tn, _op

                    # SQLite: 识别每个表是否需要 batch
                    table_ops = list(iter_table_ops(upgrade_ops.ops))
                    table_need_batch: dict[str, bool] = {}
                    for tn, _op in table_ops:
                        if not tn:
                            continue
                        if isinstance(_op, BATCH_OP_TYPES):
                            table_need_batch[tn] = True

                    current_batch_table = None
                    batch_ctx = None
                    current_runner = op_runner

                    def close_batch():
                        nonlocal batch_ctx, current_batch_table, current_runner
                        if batch_ctx:
                            batch_ctx.__exit__(None, None, None)
                            batch_ctx = None
                            current_batch_table = None
                            current_runner = op_runner  # revert

                    for tn, _op in table_ops:
                        # 无表关联的操作，直接执行
                        if not tn:
                            close_batch()
                            current_runner.invoke(_op)
                            applied = True
                            continue
                        need_batch = table_need_batch.get(tn, False)
                        if need_batch:
                            if current_batch_table != tn:
                                close_batch()
                                batch_ctx = op_runner.batch_alter_table(tn)
                                current_runner = batch_ctx.__enter__()
                                current_batch_table = tn
                        else:
                            # 该表不需要 batch，若之前有 batch 则关闭
                            if current_batch_table:
                                close_batch()
                        current_runner.invoke(_op)
                        applied = True

                    close_batch()
                    return applied

                changed = await conn.run_sync(migrate)
                if changed:
                    logger.success(f"已迁移表: {', '.join(sorted(target_tables))} (rev={revision_hash[:8]})")

    # 再处理需要删除的表
    if obsolete_tables:
        for bind_key, engine in service.engines.items():
            async with engine.begin() as conn:
                def drop(sync_conn):
                    meta = MetaData()
                    # 仅反射待删除的表, 不存在则忽略
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
        # 从 state 中移除
        for t in obsolete_tables:
            state.pop(t, None)

    # 更新/写回仍存在的表的状态
    for m in models:
        tablename = getattr(m, "__tablename__", None)
        if not tablename and hasattr(m, "__table__"):
            tablename = getattr(m.__table__, "name", None)
        if not tablename:
            continue
        state[tablename] = {"hash": revision_hash, "revision": revision_hash, "file": file_path}
    _save_state(state)
