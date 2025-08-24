from dataclasses import field
from typing import Optional, Any, Literal
from typing import Union

from sqlalchemy.ext.asyncio import create_async_engine
from arclet.letoderea.scope import global_providers, global_propagators
from arclet.entari import BasicConfModel, plugin, logger
from arclet.entari.config import config_model_validate
from arclet.entari.event.config import ConfigReload
from graia.amnesia.builtins.sqla import SqlalchemyService
from graia.amnesia.builtins.sqla.model import register_callback, remove_callback
from graia.amnesia.builtins.sqla.model import Base as Base
from graia.amnesia.builtins.sqla.types import EngineOptions
from sqlalchemy.engine.url import URL
from sqlalchemy import select as select
from sqlalchemy.ext import asyncio as sa_async
from sqlalchemy.orm import Mapped as Mapped
from sqlalchemy.orm import mapped_column as mapped_column

from .param import db_supplier, sess_provider, orm_factory
from .param import SQLDepends as SQLDepends


class UrlInfo(BasicConfModel):
    type: str = "sqlite"
    """数据库类型，默认为 sqlite"""
    name: str = "data.db"
    """数据库名称/文件路径"""
    driver: str = "aiosqlite"
    """数据库驱动，默认为 aiosqlite；其他类型的数据库驱动参考 SQLAlchemy 文档"""
    host: Optional[str] = None
    """数据库主机地址。如果是 SQLite 数据库，此项可不填。"""
    port: Optional[int] = None
    """数据库端口号。如果是 SQLite 数据库，此项可不填。"""
    username: Optional[str] = None
    """数据库用户名。如果是 SQLite 数据库，此项可不填。"""
    password: Optional[str] = None
    """数据库密码。如果是 SQLite 数据库，此项可不填。"""
    query: dict[str, Union[list[str], str]] = field(default_factory=dict)
    """数据库连接参数，默认为空字典。可以传入如 `{"timeout": "30"}` 的参数。"""

    @property
    def url(self) -> URL:
        if self.type == "sqlite":
            return URL.create(f"{self.type}+{self.driver}", database=self.name, query=self.query)
        return URL.create(
            f"{self.type}+{self.driver}", self.username, self.password, self.host, self.port, self.name, self.query
        )


class Config(UrlInfo):
    options: EngineOptions = field(default_factory=lambda: {"echo": None, "pool_pre_ping": True})
    """数据库连接选项，默认为 `{"echo": None, "pool_pre_ping": True}`"""
    session_options: Union[dict[str, Any], None] = field(default=None)
    """数据库会话选项，默认为 None。可以传入如 `{"expire_on_commit": False}` 的字典。"""
    binds: dict[str, UrlInfo]  = field(default_factory=dict)
    """数据库绑定配置，默认为 None。可以传入如 `{"bind1": UrlInfo(...), "bind2": UrlInfo(...)}` 的字典。"""
    create_table_at: Literal['preparing', 'prepared', 'blocking'] = "preparing"
    """在指定阶段创建数据库表，默认为 'preparing'。可选值为 'preparing', 'prepared', 'blocking'。"""


plugin.declare_static()
plugin.metadata(
    "Database 服务",
    [{"name": "RF-Tar-Railt", "email": "rf_tar_railt@qq.com"}],
    "0.1.1",
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

log = logger.log.wrapper("[Database]")
_config = plugin.get_config(Config)

try:
    plugin.add_service(
        service := SqlalchemyService(
            _config.url,
            _config.options,
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
    log.success("Database initialized!")
    for key, models in binds.items():
        async with serv.engines[key].begin() as conn:
            await conn.run_sync(
                serv.base_class.metadata.create_all, tables=[m.__table__ for m in models], checkfirst=True
            )
    log.success("Database tables created!")
    return True


def _setup_tablename(cls: type[Base], kwargs: dict):
    if "tablename" in kwargs:
        return
    for attr in ("__tablename__", "__table__"):
        if getattr(cls, attr, None):
            return

    cls.__tablename__ = cls.__name__.lower()

    if plg := plugin.get_plugin(3):
        cls.__tablename__ = f"{plg.id.replace('-', '_')}_{cls.__tablename__}"


register_callback(_setup_tablename)
plugin.collect_disposes(lambda: remove_callback(_setup_tablename))


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
    "SqlalchemyService"
]
