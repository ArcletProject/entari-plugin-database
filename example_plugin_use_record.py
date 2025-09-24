from typing import Sequence
from arclet.entari import PluginMetadata, Session, command, param
from entari_plugin_database import SQLDepends
from sqlalchemy import select
from example_plugin_record import Record

__plugin_metadata__ = PluginMetadata(name=__name__, depend_services=["database/sqlalchemy"])


@command.on("read")
async def read(sess: Session, recs: Sequence[Record]):
    """读取本插件的配置模型属性"""
    records = []
    for rec in recs:
        records.append((f"{rec.name} running time: {rec.end_time - rec.start_time:.2f} seconds", rec.end_time - rec.start_time))
    records.sort(key=lambda x: x[1], reverse=True)
    await sess.send(
        "Top 10 Running Times:\n" + "\n".join(f"{i+1}. {name}" for i, (name, _) in enumerate(records[:10]) if name)
    )


@command.command("check <name:str>")
async def check(plugin_name: str = param("name"), recs: Sequence[Record] = SQLDepends(select(Record).where(Record.name.startswith(param("name"))))):
    """检查某个任务的运行时间"""
    records = []
    for rec in recs:
        records.append((f"{rec.name} running time: {rec.end_time - rec.start_time:.2f} seconds", rec.end_time - rec.start_time))
    records.sort(key=lambda x: x[1], reverse=True)
    return f"Running time of plugin {plugin_name}:\n" + "\n".join(f"{i+1}. {name}" for i, (name, _) in enumerate(records) if name)
