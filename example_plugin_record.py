import time
from typing import Optional
from arclet.letoderea import Propagator
from arclet.entari.event.plugin import PluginLoadedSuccess
from arclet.entari.plugin import Plugin, PluginMetadata, get_plugin_subscribers
from entari_plugin_database import AsyncSession, BaseOrm, mapped_column, Mapped
from sqlalchemy.sql import select

plug = Plugin.current()
plug.metadata = PluginMetadata("example_plugin4")


class Record(BaseOrm):
    """A simple ORM model to record running time."""
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(nullable=False, unique=True)
    start_time: Mapped[float] = mapped_column()
    end_time: Mapped[float] = mapped_column(nullable=True)


class RecordRunningTime(Propagator):
    """A propagator to record running time."""
    def __init__(self, name: str):
        self.name = name
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None

    async def prepare(self):
        """Prepare the propagator by setting the start time."""
        self.start_time = time.time()

    async def finish(self, sess: Optional[AsyncSession] = None):
        """Finish the propagator by setting the end time."""
        self.end_time = time.time()
        if self.start_time is not None and self.end_time is not None and sess:
            async with sess.begin_nested():
                if record := (await sess.scalars(select(Record).where(Record.name == self.name))).one_or_none():
                    record.start_time = self.start_time
                    record.end_time = self.end_time
                else:
                    record = Record(name=self.name, start_time=self.start_time, end_time=self.end_time)
                    sess.add(record)

    def compose(self):
        yield self.prepare, True, 0
        yield self.finish, False, 1000


@plug.dispatch(PluginLoadedSuccess)
async def hook(event: PluginLoadedSuccess):
    """Hook to print plugin load information"""
    if event.name == plug.id:
        return
    subscribers = get_plugin_subscribers(event.name)
    for sub in subscribers:
        plug.collect(sub.propagate(RecordRunningTime(f"{sub.callable_target.__module__}.{sub.callable_target.__qualname__}")))
