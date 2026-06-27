import types
from itertools import repeat
from collections.abc import Iterable
from typing import Annotated, Any, Union, Literal, get_args, get_origin
from typing import Literal as LiteralExt


from arclet.entari import logger as log_m

logger = log_m.log.wrapper("[Database]")


def origin_is_union(origin: type[Any] | None) -> bool:
    return origin is Union or origin is types.UnionType


def origin_is_literal(origin: type[Any] | None) -> bool:
    """判断是否是 Literal 类型"""
    return origin is Literal or origin is LiteralExt


def generic_issubclass(scls: Any, cls: Any) -> bool | list[Any]:
    if isinstance(cls, tuple):
        return _map_generic_issubclass(repeat(scls), cls)

    if scls is Any:
        return [cls]

    if cls is Any:
        return True

    try:
        return issubclass(scls, cls)
    except TypeError:
        pass

    scls_origin, scls_args = get_origin(scls) or scls, get_args(scls)
    cls_origin, cls_args = get_origin(cls) or cls, get_args(cls)

    if scls_origin is tuple and cls_origin is tuple:
        if len(scls_args) == 2 and scls_args[1] is Ellipsis:
            return generic_issubclass(scls_args[0], cls_args)

        if len(cls_args) == 2 and cls_args[1] is Ellipsis:
            return _map_generic_issubclass(scls_args, repeat(cls_args[0]), failfast=True)

    if scls_origin is Annotated:
        return generic_issubclass(scls_args[0], cls)
    if cls_origin is Annotated:
        return generic_issubclass(scls, cls_args[0])

    if origin_is_union(scls_origin):
        return _map_generic_issubclass(scls_args, repeat(cls), failfast=True)
    if origin_is_union(cls_origin):
        return generic_issubclass(scls, cls_args)

    if origin_is_literal(scls_origin) and origin_is_literal(cls_origin):
        return set(scls_args) <= set(cls_args)

    try:
        if not issubclass(scls_origin, cls_origin):
            return False
    except TypeError:
        return False

    if not cls_args:
        return True

    if len(scls_args) != len(cls_args):
        return False

    return _map_generic_issubclass(scls_args, cls_args, failfast=True)


def _map_generic_issubclass(scls: Iterable[Any], cls: Iterable[Any], *, failfast: bool = False) -> bool | list[Any]:
    results = []
    for scls_arg, cls_arg in zip(scls, cls):
        if not (result := generic_issubclass(scls_arg, cls_arg)) and failfast:
            return False
        elif isinstance(result, list):
            results.extend(result)
        elif not isinstance(result, bool):
            results.append(result)

    return results or False
