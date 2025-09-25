from dataclasses import dataclass, field
from inspect import Signature, Parameter
from operator import methodcaller

from collections.abc import Iterator, Sequence, AsyncIterator

from sqlalchemy import Row, Result, ScalarResult, select
from sqlalchemy.ext.asyncio import AsyncResult, AsyncScalarResult
from sqlalchemy.sql.selectable import ExecutableReturnsRows
from tarina import generic_issubclass
from tarina.generic import origin_is_union, isclass
from typing_extensions import Any
from typing import cast, get_args, get_origin

from creart import it
from launart import Launart
from graia.amnesia.builtins.sqla import SqlalchemyService, Base

from arclet.letoderea import Propagator, Contexts, STACK, Provider, ProviderFactory, Param, Depend, Subscriber
from arclet.letoderea.ref import Deref, generate
from arclet.letoderea.provider import global_providers
from arclet.letoderea.scope import global_propagators
from sqlalchemy.ext import asyncio as sa_async


class DatabasePropagator(Propagator):
    def validate(self, subscriber: Subscriber):
        params = subscriber.params
        if any((p.depend and isinstance(p.depend, SQLDepend)) for p in params):
            return True
        if any(isinstance(prod, (SessionProvider, ORMProviderFactory._ModelProvider)) for p in params for prod in p.providers):  # noqa: E501,UP038
            return True
        return False

    async def supply(self, ctx: Contexts, serv: SqlalchemyService | None = None):
        if serv is None:
            return
        session = serv.get_session()
        stack = ctx[STACK]
        session = await stack.enter_async_context(session)
        return {"$db_session": session}

    def compose(self):
        yield self.supply, True, 20


class SessionProvider(Provider[sa_async.AsyncSession]):
    priority = 10

    async def __call__(self, context: Contexts):
        if "$db_session" in context:
            return context["$db_session"]
        try:
            db = it(Launart).get_component(SqlalchemyService)
            stack = context[STACK]
            sess = await stack.enter_async_context(db.get_session())
            context["$db_session"] = sess
            return sess
        except ValueError:
            return


@dataclass(unsafe_hash=True)
class Option:
    stream: bool = True
    scalars: bool = False
    calls: tuple[methodcaller, ...] = field(default_factory=tuple)
    result: methodcaller | None = None


PATTERNS = {
    AsyncIterator[Sequence[Row[tuple[Any, ...]]]]: Option(
        True,
        False,
        (methodcaller("partitions"),),
    ),
    AsyncIterator[Sequence[tuple[Any, ...]]]: Option(
        True,
        False,
        (methodcaller("partitions"),),
    ),
    AsyncIterator[Sequence[Any]]: Option(
        True,
        True,
        (methodcaller("partitions"),),
    ),
    Iterator[Sequence[Row[tuple[Any, ...]]]]: Option(
        False,
        False,
        (methodcaller("partitions"),),
    ),
    Iterator[Sequence[tuple[Any, ...]]]: Option(
        False,
        False,
        (methodcaller("partitions"),),
    ),
    Iterator[Sequence[Any]]: Option(
        False,
        True,
        (methodcaller("partitions"),),
    ),
    AsyncResult[tuple[Any, ...]]: Option(
        True,
        False,
    ),
    AsyncScalarResult[Any]: Option(
        True,
        True,
    ),
    Result[tuple[Any, ...]]: Option(
        False,
        False,
    ),
    ScalarResult[Any]: Option(
        False,
        True,
    ),
    AsyncIterator[Row[tuple[Any, ...]]]: Option(
        True,
        False,
    ),
    Iterator[Row[tuple[Any, ...]]]: Option(
        False,
        False,
    ),
    Sequence[Row[tuple[Any, ...]]]: Option(
        True,
        False,
        (),
        methodcaller("all"),
    ),
    Sequence[tuple[Any, ...]]: Option(
        True,
        False,
        (),
        methodcaller("all"),
    ),
    Sequence[Any]: Option(
        True,
        True,
        (),
        methodcaller("all"),
    ),
    tuple[Any, ...]: Option(
        True,
        False,
        (),
        methodcaller("one_or_none"),
    ),
    Any: Option(
        True,
        True,
        (),
        methodcaller("one_or_none"),
    ),
}


class SQLDepend(Depend):
    def __init__(self, statement: ExecutableReturnsRows, option: Option = Option(), cache: bool = False):
        super().__init__(lambda : None, cache)
        self.statement = statement
        self.option = option

        async def target(db_session: sa_async.AsyncSession,  **params):
            if self.option.stream:
                result = await db_session.stream(self.statement, params)
            else:
                result = await db_session.execute(self.statement, params)
            if self.option.scalars:
                result = result.scalars()
            for call in self.option.calls:
                result = call(result)
            if call := self.option.result:
                result = call(result)
                if self.option.stream:
                    result = await result
            return result

        parameters = [Parameter("db_session", Parameter.KEYWORD_ONLY, annotation=sa_async.AsyncSession)]
        for name, depends in self.statement.compile().params.items():
            if isinstance(depends, Depend):
                parameters.append(Parameter(name, Parameter.KEYWORD_ONLY, default=depends))
            elif isinstance(depends, Deref):
                parameters.append(Parameter(name, Parameter.KEYWORD_ONLY, default=Depend(generate(depends))))
        target.__signature__ = Signature(parameters)  # type: ignore
        self.target = target

    def fork(self, provider: list[Provider | ProviderFactory]):
        if hasattr(self, "sub"):  # pragma: no cover
            return self
        self.sub = Subscriber(self.target, providers=provider)
        return self


def SQLDepends(statement: ExecutableReturnsRows, option: Option = Option(), cache: bool = False) -> Any:
    return SQLDepend(statement, option, cache)


class ORMProviderFactory(ProviderFactory):
    priority = 10

    class _ModelProvider(Provider[Any]):
        def __init__(self, statement: ExecutableReturnsRows, option: Option):
            super().__init__()
            self.statement = statement
            self.option = option

        async def __call__(self, context: Contexts):
            if "$db_session" not in context:
                return
            sess: sa_async.AsyncSession = context["$db_session"]
            if self.option.stream:
                result = await sess.stream(self.statement)
            else:
                result = await sess.execute(self.statement)
            if self.option.scalars:
                result = result.scalars()
            for call in self.option.calls:
                result = call(result)
            if call := self.option.result:
                result = call(result)
                if self.option.stream:
                    result = await result
            return result

    def validate(self, param: Param):
        for pattern, option in PATTERNS.items():
            if models := cast("list[Any]", generic_issubclass(pattern, param.annotation, list_=True)):
                break
        else:
            models, option = [], Option()
        if isinstance(param.default, SQLDepend):
            param.default.option = option
            return
        for index, model in enumerate(models):
            if origin_is_union(get_origin(model)):
                models[index] = next(
                    (
                        arg
                        for arg in get_args(model)
                        if isclass(arg) and issubclass(arg, Base)
                    ),
                    None,
                )

            if not (isclass(models[index]) and issubclass(models[index], Base)):
                models = []
                break
        if not models:
            return

        statement = select(*models)
        return self._ModelProvider(statement, option)


global_propagators.append(db_supplier := DatabasePropagator())
global_providers.append(sess_provider := SessionProvider())
global_providers.append(orm_factory := ORMProviderFactory())
