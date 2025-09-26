# entari-plugin-database

Entari plugin for SQLAlchemy ORM

## 功能

- 提供数据库服务支持，管理多个数据库连接
- 依赖注入会话与 ORM 模型数据
- 依据类型注解或 SQLDepends 的复杂依赖关系注入
  - 例如 `Sequence[Model]` 会注入所有模型
  - 声明 `SQLDepends(select(Model).where(Model.field == value))` 会注入查询结果
  - 通过配合 `arclet.entari.param`, 可以在 `SQLDepends` 的查询语句中使用 `param` 声明其他的依赖注入参数
- 自动数据库迁移
  - 通过 `alembic` 实现
  - 检测到模型变更时，自动生成并应用迁移脚本
  - 根据模型的 `__bind_key__` 属性，支持多数据库迁移
  - 允许开发者自定义迁移脚本

### 多数据库

- 插件配置项 `binds` 可以根据 bind_key 声明多个数据库连接
- ORM 模型可以通过 `__bind_key__` 属性指定使用哪个数据库连接
- 默认数据库连接使用 `""` 作为 `bind_key`

### 迁移

- ORM 模型的变更会被自动检测并生成迁移脚本
- 本插件会根据模型的结构自动计算 `revision`
- ORM 模型可以声明 `__revision__` 属性来指定特定的 `revision`
- 迁移记录将存储在用户目录下的 `.entari/data/database/migrations_lock.json` 文件中


## 配置

- `type`：数据库类型，默认为 `sqlite`。
- `name`：数据库名称或文件路径，默认为 `data.db`。
- `driver`：数据库驱动，默认为 `aiosqlite`；其他类型的数据库驱动参考 SQLAlchemy 文档。
- `host`：数据库主机地址。如果是 SQLite 数据库，此项可不填。
- `port`：数据库端口号。如果是 SQLite 数据库，此项可不填。
- `username`：数据库用户名。如果是 SQLite 数据库，此项可不填。
- `password`：数据库密码。如果是 SQLite 数据库，此项可不填。
- `query`：数据库连接参数，默认为空字典。可以传入如 `{"timeout": "30"}` 的参数。
- `session_options`：数据库会话选项，默认为 None。可以传入如 `{"expire_on_commit": False}` 的字典。
- `binds`：数据库绑定配置，默认为 None。可以传入如 `{"bind1": UrlInfo(...), "bind2": UrlInfo(...)}` 的字典。
  ```yaml
  plugins:
    database:
      type: sqlite
      binds:
        foo:
          type: mysql
          driver: asyncmy
          host: localhost
          port: 3306
          name: foo_db
          username: root
          password: password
  ``` 
- `options`：数据库连接选项，默认为 `{"echo": None, "pool_pre_ping": True}`。
- `create_table_at`：在指定阶段创建数据库表，默认为 'preparing'。可选值为 'preparing', 'prepared', 'blocking'。
