import asyncio

from sqlalchemy.ext.asyncio import create_async_engine
from arclet.letoderea.provider import global_providers
from arclet.letoderea.scope import global_propagators
from arclet.letoderea.core import add_task
from arclet.entari import plugin
from arclet.entari.config import config_model_validate
from arclet.entari.event.config import ConfigReload
from graia.amnesia.builtins.sqla import SqlalchemyService
from graia.amnesia.builtins.sqla.model import register_callback, remove_callback
from graia.amnesia.builtins.sqla.model import Base as Base

from sqlalchemy import select as select
from sqlalchemy.ext import asyncio as sa_async
from sqlalchemy.orm import Mapped as Mapped
from sqlalchemy.orm import mapped_column as mapped_column

from .param import db_supplier, sess_provider, orm_factory
from .param import SQLDepends as SQLDepends
from .utils import logger
from .migration import _LOCK, _MODULE_MODELS, run_migration_for, register_custom_migration
from .config import Config


plugin.declare_static()
plugin.metadata(
    "Database 服务",
    [{"name": "RF-Tar-Railt", "email": "rf_tar_railt@qq.com"}],
    "0.2.0",
    description="基于 SQLAlchemy 的数据库服务插件",
    urls={
        "homepage": "https://github.com/ArcletProject/entari-plugin-database",
    },
    config=Config,
)
plugin.collect_disposes(
    lambda: global_propagators.remove(db_supplier),
    lambda: global_providers.remove(sess_provider),
    lambda: global_providers.remove(orm_factory),
)

_config = plugin.get_config(Config)

try:
    plugin.add_service(
        service := SqlalchemyService(
            _config.url,
            _config.options,  # type: ignore
            _config.session_options,
            {key: value.url for key, value in _config.binds.items()},
            _config.create_table_at
        )
    )
except Exception as e:
    raise RuntimeError("Failed to initialize SqlalchemyService. Please check your database configuration.") from e


@plugin.listen(ConfigReload)
async def reload_config(event: ConfigReload, serv: SqlalchemyService):
    if event.scope != "plugin":
        return None
    if event.key not in ("database", "entari_plugin_database"):
        return None
    new_conf = config_model_validate(Config, event.value)
    for engine in serv.engines.values():
        await engine.dispose(close=True)
    engine_options = {"echo": "debug", "pool_pre_ping": True}
    serv.engines = {"": create_async_engine(new_conf.url, **(new_conf.options or engine_options))}
    for key, bind in (new_conf.binds or {}).items():
        serv.engines[key] = create_async_engine(bind.url, **(new_conf.options or engine_options))
    serv.create_table_at = new_conf.create_table_at
    serv.session_options = new_conf.session_options or {"expire_on_commit": False}

    binds = await serv.initialize()
    logger.success("Database initialized!")
    for key, models in binds.items():
        async with serv.engines[key].begin() as conn:
            await conn.run_sync(
                serv.base_class.metadata.create_all, tables=[m.__table__ for m in models], checkfirst=True
            )
    logger.success("Database tables created!")
    return True


def _clean_exist(cls: type[Base], kwargs: dict):
    if cls.__tablename__ in Base.metadata.tables:
        cls.registry.dispose()
        dict.pop(Base.metadata.tables, cls.__tablename__, None)


def _setup_tablename(cls: type[Base], kwargs: dict):
    if "tablename" in kwargs:
        cls.__tablename__ = kwargs["tablename"]
        return
    for attr in ("__tablename__", "__table__"):
        if getattr(cls, attr, None):
            return

    cls.__tablename__ = cls.__name__.lower()

    if plg := plugin.get_plugin(3):
        cls.__tablename__ = f"{plg.id.replace('-', '_')}_{cls.__tablename__}"


_PENDING_MODEL_TASK: set[str] = set()


def migration_callback(cls: type[Base], kwargs: dict):
    # 只有拥有 __tablename__ / __table__ 的实体才考虑
    if not (hasattr(cls, "__tablename__") or hasattr(cls, "__table__")):
        return
    module = cls.__module__
    with _LOCK:
        _MODULE_MODELS.setdefault(module, set()).add(cls)
        # 避免同一文件重复并发执行, 使用一次性调度
        if module in _PENDING_MODEL_TASK:
            return
        _PENDING_MODEL_TASK.add(module)

    async def _delayed():
        # 给同一文件内后续类定义一点时间注册
        await asyncio.sleep(0)  # 让出事件循环
        try:
            await run_migration_for(module, service)
        except Exception as e:
            logger.exception(f"[Migration] 处理模块 {module} 失败: {e}", exc_info=e)
        finally:
            with _LOCK:
                _PENDING_MODEL_TASK.discard(module)

    add_task(_delayed())


register_callback(_setup_tablename)
register_callback(_clean_exist)
register_callback(migration_callback)
plugin.collect_disposes(lambda: remove_callback(_clean_exist))
plugin.collect_disposes(lambda: remove_callback(_setup_tablename))
plugin.collect_disposes(lambda: remove_callback(migration_callback))


BaseOrm = Base
AsyncSession = sa_async.AsyncSession
get_session = service.get_session


__all__ = [
    "AsyncSession",
    "Base",
    "BaseOrm",
    "Mapped",
    "mapped_column",
    "service",
    "SQLDepends",
    "get_session",
    "select",
    "SqlalchemyService",
    "register_custom_migration",
]
