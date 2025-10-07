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
from sqlalchemy.orm import Mapped as Mapped, instrumentation
from sqlalchemy.orm import mapped_column as mapped_column

from .param import db_supplier, sess_provider, orm_factory
from .param import SQLDepends as SQLDepends
from .utils import logger
from .migration import run_migration, register_custom_migration
from .config import Config


plugin.declare_static()
plugin.metadata(
    "Database 服务",
    [{"name": "RF-Tar-Railt", "email": "rf_tar_railt@qq.com"}],
    "0.2.1",
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
    existing_table = Base.metadata.tables.get(cls.__tablename__)
    if existing_table is None:
        return
    Base.metadata.remove(existing_table)
    for manager, _ in Base.registry._managers.items():
        class_ = manager.class_
        if repr(class_) == repr(cls) and class_ is not cls:
            # 清理已失效的类定义
            if "mapper" in manager.__dict__ and manager.mapper is not None:
                manager.mapper._set_dispose_flags()
            Base.registry._dispose_cls(class_)
            instrumentation._instrumentation_factory.unregister(class_)
            Base.registry._managers.pop(manager, None)
            break


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


_PENDING_TASKS = set()


def migration_callback(cls: type[Base], kwargs: dict):
    if _PENDING_TASKS:
        return

    async def _delayed():
        # 给同一文件内后续类定义一点时间注册
        await service.status.wait_for("blocking")
        try:
            await run_migration(service)
        except Exception as e:
            logger.exception(f"[Migration] 迁移失败: {e}", exc_info=e)

    task = add_task(_delayed())
    _PENDING_TASKS.add(task)
    task.add_done_callback(_PENDING_TASKS.discard)


register_callback(_setup_tablename)
register_callback(_clean_exist)
register_callback(migration_callback, after=True)
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

# logger.disable("alembic.runtime.migration")
