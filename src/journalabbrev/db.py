from deepmerge import Merger, STRATEGY_END
from deepmerge.exception import InvalidMerge
from deepmerge.strategy.dict import DictStrategies
from deepmerge.strategy.fallback import FallbackStrategies
from deepmerge.strategy.list import ListStrategies
from deepmerge.strategy.type_conflict import TypeConflictStrategies
import os.path
from packaging.version import Version
from pymitter import EventEmitter
import re
from re import RegexFlag
from tinydb import TinyDB, Query
from tinydb.middlewares import CachingMiddleware
import tinydb.operations as dbops
from tinydb.storages import JSONStorage
from tinydb.table import Document, Table
from typing import *
from varname import argname

from . import *
from .common import *


_MetadataQuery = Query()
_JournalQuery = Query()

JournalId = Table.document_id_class


class DBObject():
	_keys: List[str] = None

	def __init__(self, doc: Mapping = None):
		self._doc = doc or cast(Mapping, dict())

	def __repr__(self) -> str:
		return f"<{type(self).__name__} {self.name!r}>"

	def asdict(self) -> Mapping:
		return self._doc


class DBObjectMeta(type):
	def __new__(cls, name: str, bases: Tuple[type, ...], namespace: Dict[str, Any], **kwds: Any) -> type:
		o = super().__new__(cls, name, bases, namespace, **kwds)

		def create_key_property(key: str):
			@property
			def key_getter(self: DBObject) -> str:
				return self._doc.get(key)

			@key_getter.setter
			def key_setter(self: DBObject, value) -> str:
				self._doc[key] = value

			return key_getter, key_setter

		for cur_key in o._keys:
			getter, setter = create_key_property(cur_key)
			setattr(o, cur_key, getter)
			setattr(o, cur_key, setter)
			setattr(o, f"{cur_key}_key", cur_key)

		return o


class Journal(DBObject, metaclass = DBObjectMeta):
	_keys = ["names", "issn", "iso4", "coden"]


class JournalsList():
	@staticmethod
	def _get_db_query(key: str, value: Union[str, Pattern]):
		query_key = _JournalQuery[key]
		if isinstance(value, str):
			return query_key.test(lambda v: v.casefold() == cast(str, value).casefold())
		elif isinstance(value, Pattern):
			return query_key.matches(cast(Pattern, value))
		else:
			raise ValueError(f"Argument '{argname(value)}' has invald type")

	@staticmethod
	def _merge_strategy_xor(config, path, base, nxt):
		if base is None:
			return nxt
		if nxt is None:
			return base
		if type(base) == type(nxt) and base == nxt:
			return base
		return STRATEGY_END

	def __init__(self, journals_table: Table):
		self._journals_table = journals_table

	def __repr__(self):
		return f"<{type(self).__name__} count={len(self)!r}>"

	def __iter__(self) -> Iterator[Journal]:
		for doc in self._journals_table:
			yield Journal(doc)

	def __len__(self):
		return len(self._journals_table)

	def contains(self, key: str, value: Union[str, Pattern]) -> bool:
		return self._journals_table.contains(self._get_db_query(key, value))

	def get(self, key: str, value: Union[str, Pattern]) -> Journal:
		doc = self._journals_table.get(self._get_db_query(key, value))
		return Journal(doc) if doc is not None else None

	def get_all(self, key: str, value: Union[str, Pattern]) -> Journal:
		docs = cast(List[Document], self._journals_table.search(self._get_db_query(key, value)))
		return (Journal(d) for d in docs)

	def add(self, journal: Journal) -> JournalId:
		return self._journals_table.insert(journal._doc)

	def remove(self, key: str, value: Union[str, Pattern]) -> List[JournalId]:
		return self._journals_table.remove(self._get_db_query(key, value))

	def update(self, journal: Journal) -> List[JournalId]:
		return self._journals_table.update(journal._doc, self._get_db_query(Journal.name_key, journal.name))

	def merge(self, journal: Journal, overwrite: bool = False) -> JournalId:
		merger = Merger(
			[
				(dict, [DictStrategies.strategy_merge]),
			],
			[FallbackStrategies.strategy_override if overwrite else self._merge_strategy_xor],
			[TypeConflictStrategies.strategy_override if overwrite else self._merge_strategy_xor],
		)

		def update_op(doc):
			merger.merge(doc, journal._doc)

		return self._journals_table.update(update_op, self._get_db_query("name", journal.name))


class JournalDB(EventEmitter):
	_format_version_key = "format.version"

	latest_format_version = Version(__version__)

	def __init__(self):
		super().__init__(self)

		self._filename = os.path.join(app_user_data_dir, "db.json")
		self._db = None

	def __repr__(self) -> str:
		return f"<{type(self).__name__} format_version={self.format_version()!r} journals={self.journals()!r}>"

	def __enter__(self):
		return self

	def __exit__(self, *args):
		self.close()

	def _upgrade_format(self, cur_format_version: Version):
		self.emit("upgrade_started", cur_format_version, self.latest_format_version, len(self._journals_table))

		def update_op(doc):
			if cur_format_version <= Version("0.1.0"):
				if not "names" in doc:
					name = doc.get("name")
					doc["names"] = [name] if name is not None else []
				del doc["name"]

		self.emit("upgrade_progress", 0)

		num_processed = 0
		for doc in self._journals_table:
			doc_ids = [doc.doc_id]
			updated_doc_ids = self._journals_table.update(update_op, doc_ids = doc_ids)
			assert(updated_doc_ids == doc_ids)

			num_processed += 1
			self.emit("upgrade_progress", num_processed)

		self._set_metadata(self._format_version_key, str(self.latest_format_version))

		self.emit("upgrade_finished", self.latest_format_version, len(self._journals_table))

	def open(self):
		self._db = TinyDB(self._filename, storage = CachingMiddleware(JSONStorage))
		self._metadata_table = cast(Table, self._db.table("metadata"))
		self._journals_table = cast(Table, self._db.table("journals"))
		self._journals_list = JournalsList(self._journals_table)

		cur_format_version_str = self._get_metadata(self._format_version_key)
		cur_format_version = Version(cur_format_version_str) if cur_format_version_str is not None else cur_format_version_str

		if cur_format_version is None:
			# Datbase was just created.

			self._set_metadata(self._format_version_key, str(self.latest_format_version))

			self.emit("created", self.latest_format_version)
		elif cur_format_version < self.latest_format_version:
			# Database uses outdated format.

			self._upgrade_format(cur_format_version)

		self.flush()

	def close(self):
		if self._db is not None:
			self._db.close()
			self._db = None

	def _check_open(self) -> bool:
		return self._db is not None

	def flush(self):
		self._check_open()

		if isinstance(self._db.storage, CachingMiddleware):
			storage = cast(CachingMiddleware, self._db.storage)
			storage.flush()

	def _has_metadata(self, key: str) -> bool:
		return self._metadata_table.contains(_MetadataQuery.key == key)

	def _get_metadata(self, key: str) -> Document:
		doc = self._metadata_table.get(_MetadataQuery.key == key)
		return doc.get("value") if doc is not None else None

	def _set_metadata(self, key: str, value: Any):
		doc = {
			"key": key,
			"value": value,
		}
		return self._metadata_table.upsert(doc, _MetadataQuery.key == key)

	def format_version(self):
		self._check_open()

		version = self._get_metadata(self._format_version_key)
		return Version(version) if version is not None else version

	def journals(self):
		self._check_open()

		return self._journals_list
