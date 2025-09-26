from dataclasses import field
from typing import Any, Literal, TypedDict

from arclet.entari import BasicConfModel
from sqlalchemy.engine.interfaces import IsolationLevel, _ParamStyle
from sqlalchemy.log import _EchoFlagType
from sqlalchemy.pool.base import ResetStyle
from sqlalchemy.engine.url import URL


class EngineOptions(TypedDict, total=False):
    connect_args: dict[Any, Any]
    """传递给数据库驱动的连接参数字典"""
    echo: _EchoFlagType
    """如果为 True，启用 SQL 语句日志记录；如果为 "debug"，则记录更详细的调试信息"""
    echo_pool: _EchoFlagType
    """如果为 True，启用连接池日志记录；如果为 "debug"，则记录更详细的调试信息"""
    enable_from_linting: bool
    """启用 FROM 子句检查，默认为 True"""
    execution_options: dict[str, Any]
    """执行选项字典"""
    future: Literal[True]
    """使用 2.0 风格的 Engine 和 Connection API"""
    hide_parameters: bool
    """如果为 True，日志中不显示 SQL 参数"""
    implicit_returning: Literal[True]
    """启用隐式返回，默认为 True"""
    insertmanyvalues_page_size: int
    """每页插入的行数，默认为 1000"""
    isolation_level: IsolationLevel
    """设置连接的隔离级别"""
    label_length: int | None
    """限制动态生成的列标签的长度"""
    logging_name: str
    """日志记录名称"""
    max_identifier_length: int | None
    """最大标识符长度"""
    max_overflow: int
    """连接池的最大溢出连接数"""
    module: Any | None
    """数据库驱动模块"""
    paramstyle: _ParamStyle | None
    """参数样式"""
    pool_logging_name: str
    """连接池日志记录名称"""
    pool_pre_ping: bool
    """启用连接池预检测，默认为 True"""
    pool_size: int
    """连接池大小"""
    pool_recycle: int
    """连接池连接的最大重用时间"""
    pool_reset_on_return: ResetStyle | bool | Literal["commit", "rollback"] | None
    """连接池连接返回时的重置策略"""
    pool_timeout: float
    """连接池获取连接的超时时间"""
    pool_use_lifo: bool
    """是否使用 LIFO 策略获取连接"""
    plugins: list[str]
    """要加载的插件列表"""
    query_cache_size: int
    """查询缓存大小"""
    use_insertmanyvalues: bool
    """启用 insertmanyvalues 执行风格，默认为 True"""
    skip_autocommit_rollback: bool
    """如果为 True，则在自动提交模式下跳过回滚操作"""
    kwargs: dict[str, Any]
    """其他额外参数"""


class UrlInfo(BasicConfModel):
    type: str = "sqlite"
    """数据库类型，默认为 sqlite"""
    name: str = "data.db"
    """数据库名称/文件路径"""
    driver: str = "aiosqlite"
    """数据库驱动，默认为 aiosqlite；其他类型的数据库驱动参考 SQLAlchemy 文档"""
    host: str | None = None
    """数据库主机地址。如果是 SQLite 数据库，此项可不填。"""
    port: int | None = None
    """数据库端口号。如果是 SQLite 数据库，此项可不填。"""
    username: str | None = None
    """数据库用户名。如果是 SQLite 数据库，此项可不填。"""
    password: str | None = None
    """数据库密码。如果是 SQLite 数据库，此项可不填。"""
    query: dict[str, list[str] | str] = field(default_factory=dict)
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
    session_options: dict[str, Any] | None = field(default=None)
    """数据库会话选项，默认为 None。可以传入如 `{"expire_on_commit": False}` 的字典。"""
    binds: dict[str, UrlInfo] = field(default_factory=dict)
    """数据库绑定配置，默认为 None。可以传入如 `{"bind1": UrlInfo(...), "bind2": UrlInfo(...)}` 的字典。"""
    create_table_at: Literal["preparing", "prepared", "blocking"] = "preparing"
    """在指定阶段创建数据库表，默认为 'preparing'。可选值为 'preparing', 'prepared', 'blocking'。"""
