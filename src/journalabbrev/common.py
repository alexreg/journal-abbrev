from appdirs import *
import os.path
from os import PathLike
from re import Match, Pattern
from typing import *


T = TypeVar('T')

if not TYPE_CHECKING:
	IO = Any
	BinaryIO = Any
	TextIO = Any


class ProcessingError(Exception):
	def __init__(self, message: str):
		super().__init__(message)


class MergeConflict(Exception):
	def __init__(self, *args: object):
		super().__init__(*args)


def sub_or_none(pattern: Pattern, repl: Union[AnyStr, Callable[[Match[AnyStr]], AnyStr]], string: AnyStr, count: int = 0) -> AnyStr:
	return pattern.sub(repl, string, count) if string is not None else None


def ensure_dir(path: PathLike) -> PathLike:
	if not os.path.exists(path):
		os.mkdir(path)
	return path


def cache_in_memory(io: BinaryIO, size = None):
	from io import BytesIO
	mem_buf = BytesIO()
	if size is not None:
		mem_buf.truncate(size)
	for chunk in io.iter_content(chunk_size = 0xFFF):
		mem_buf.write(chunk)
	mem_buf.seek(0)
	return mem_buf


def cache_in_fs(io: BinaryIO):
	from tempfile import TemporaryFile
	tmp_file = TemporaryFile()
	for chunk in io.iter_content(chunk_size = 0xFFF):
		tmp_file.write(chunk)
	tmp_file.seek(0)
	return tmp_file


app_name = "journal-abbrev"
app_author = None
app_user_data_dir = ensure_dir(user_data_dir(app_name, app_author or False))
app_user_cache_dir = ensure_dir(user_cache_dir(app_name, app_author or False))
app_user_log_dir = ensure_dir(user_log_dir(app_name, app_author or False))
