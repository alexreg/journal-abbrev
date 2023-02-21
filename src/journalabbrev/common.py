import os.path
from functools import cache
from io import BytesIO
from os import PathLike
from re import Match, Pattern
from typing import *

from appdirs import *
from deepmerge import STRATEGY_END
from deepmerge.strategy.core import StrategyList


if not TYPE_CHECKING:
    IO = Any
    BinaryIO = Any
    TextIO = Any

ClassVarOrigin: Type = get_origin(ClassVar[Any])

T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")


class ProcessingError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)


class MergeConflict(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class SetStrategies(StrategyList):
    """
    Contains the strategies provided for sets.
    """

    NAME = "set"

    @staticmethod
    def strategy_override(config, path, base, nxt):
        """Use the set nxt."""
        return nxt

    @staticmethod
    def strategy_union(config, path, base, nxt):
        """Unify nxt with base."""
        return base | nxt


def try_int(x: str, base: int = 10) -> Optional[int]:
    if x is None:
        return None

    try:
        return int(x, base)
    except ValueError:
        return None


def class_init(cls: Type) -> Type:
    if getattr(cls, "__class_init__", None):
        cls.__class_init__()

    return cls


def get_pub_attrs(obj: Any) -> Dict[str, Type]:
    attrs = {}
    for name, typ in get_type_hints(obj).items():
        origin_typ = get_origin(typ) or typ
        if origin_typ is Union:
            origin_typ = get_args(typ)[0]
        if origin_typ and origin_typ is not ClassVarOrigin:
            attrs[name] = origin_typ

    return attrs


@cache
def get_pub_attrs_cached(obj: Any) -> Dict[str, Type]:
    return get_pub_attrs(obj)


def normalize_regex(pattern: str) -> str:
    return "".join(line.lstrip() for line in pattern.splitlines())


def sub_or_none(
    pattern: Pattern,
    repl: Union[AnyStr, Callable[[Match[AnyStr]], AnyStr]],
    string: AnyStr,
    count: int = 0,
) -> AnyStr:
    return pattern.sub(repl, string, count) if string is not None else None


def ensure_dir(path: PathLike) -> PathLike:
    if not os.path.exists(path):
        os.mkdir(path)

    return path


def cache_in_memory(io: BinaryIO, size=None) -> BytesIO:
    mem_buf = BytesIO()
    if size is not None:
        mem_buf.truncate(size)
    for chunk in io.iter_content(chunk_size=0xFFF):
        mem_buf.write(chunk)

    mem_buf.seek(0)
    return mem_buf


def cache_in_fs(io: BinaryIO) -> IO:
    from tempfile import TemporaryFile

    tmp_file = TemporaryFile()
    for chunk in io.iter_content(chunk_size=0xFFF):
        tmp_file.write(chunk)

    tmp_file.seek(0)
    return tmp_file


def merge_strategy_xor(config, path, base, nxt):
    if base is None:
        return nxt
    if nxt is None:
        return base
    if type(base) == type(nxt) and base == nxt:
        return base

    return STRATEGY_END


app_name = "journal-abbrev"
app_author = None
app_user_data_dir = ensure_dir(user_data_dir(app_name, app_author or False))
app_user_cache_dir = ensure_dir(user_cache_dir(app_name, app_author or False))
app_user_log_dir = ensure_dir(user_log_dir(app_name, app_author or False))
