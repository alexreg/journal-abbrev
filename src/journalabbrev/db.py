import os.path
import re
from io import StringIO
from itertools import islice
from re import Pattern, RegexFlag
from sys import prefix
from typing import *

import msgpack
import rocksdb
import rocksdb.errors
from deepmerge import STRATEGY_END, Merger
from deepmerge.exception import InvalidMerge
from deepmerge.strategy.dict import DictStrategies
from deepmerge.strategy.fallback import FallbackStrategies
from deepmerge.strategy.list import ListStrategies
from deepmerge.strategy.type_conflict import TypeConflictStrategies
from packaging.version import Version
from pymitter import EventEmitter
from rocksdb.interfaces import *
from varname import argname

from . import *
from .common import *

JournalID = NewType("JournalID", int)

_EncodeFn = Callable[[Any], Dict]
_DecodeFn = Callable[[Dict], Any]

_ignorable_words_regex = re.compile(normalize_regex(r"""
	\b(?:(the|a|le|la|les|li|gli|el|los|las|der|die|das)\s|(l)')
"""), flags = RegexFlag.IGNORECASE)


class DBObjectMeta(type):
	def __new__(cls, name: str, bases: Tuple[type, ...], namespace: Dict[str, Any], **kwds: Any) -> type:
		from inspect import isclass

		obj = super().__new__(cls, name, bases, namespace, **kwds)

		slots = []
		collection_attrs = set()
		for attr_name, attr_typ in get_pub_attrs(obj).items():
			slots.append(attr_name)
			if issubclass(attr_typ, Collection):
				collection_attrs.add(attr_name)
			setattr(obj, f"{attr_name}_key", attr_name)

		obj.__slots__ = slots
		obj._collection_attrs = collection_attrs

		return obj


class DBObject():
	_collection_attrs: ClassVar[Set[str]] = None

	@staticmethod
	def _merge_strategy(config, path, base: 'DBObject', nxt: 'DBObject') -> 'DBObject':
		base_attrs = get_pub_attrs(base)
		for attr_name in get_pub_attrs(nxt):
			attr_value = getattr(nxt, attr_name)
			base_attr_value = getattr(base, attr_name)
			if not base_attr_value:
				setattr(base, attr_name, attr_value)
			else:
				setattr(base, attr_name, config.value_strategy(path + [attr_name], base_attr_value, attr_value))
		return base

	@classmethod
	def fromdict(cls, d: Dict[str, Any]) -> 'DBObject':
		o = cls()
		o.__dict__ = d
		return o

	def __init__(self):
		pass

	def __repr__(self) -> str:
		attr_values = "".join(f" {name}={value!r}" for name, value in self.asdict().items())
		return f"<{type(self).__name__}{attr_values}>"

	def asdict(self) -> Dict[str, Any]:
		attrs = ((name, getattr(self, name)) for name, _ in get_pub_attrs(self).items())
		return { name: value for name, value in attrs if value is not None }


@class_init
class Journal(DBObject, metaclass = DBObjectMeta):
	_merger: ClassVar[Merger] = None

	names: Set[str] = None
	issn_print: Optional[str] = None
	issn_web: Optional[str] = None
	iso4: Optional[str] = None
	coden: Optional[str] = None

	@classmethod
	def __class_init__(cls):
		cls._merger = Merger(
			[
				(set, SetStrategies.strategy_union),
				(list, ListStrategies.strategy_append),
				(dict, DictStrategies.strategy_merge),
				(DBObject, DBObject._merge_strategy),
			],
			[merge_strategy_xor],
			[merge_strategy_xor],
		)

	@classmethod
	def merge(cls, base: 'Journal', nxt: 'Journal') -> 'Journal':
		return cls._merger.merge(base, nxt)

	def __init__(self):
		super().__init__()
		self.names = set()

	def matches(self, key: str, value: Union[str, Collection[str], Pattern[str]]) -> bool:
		if isinstance(value, str):
			value = value.casefold()
			if key in self._collection_attrs:
				collection = cast(Collection, getattr(self, key))
				return any(el.casefold() == value for el in collection)
			else:
				self_value = cast(str, getattr(self, key)).casefold()
				return self_value == value
		elif isinstance(value, Collection):
			value = cast(Collection[str], value)
			if key in self._collection_attrs:
				collection = cast(Collection, getattr(self, key))
				return any(el.casefold() == value_el.casefold() for el in collection for value_el in value)
			else:
				self_value = cast(str, getattr(self, key)).casefold()
				return self_value == value
		elif isinstance(value, Pattern):
			pattern = cast(Pattern[str], value)
			if key in self._collection_attrs:
				collection = cast(Collection, getattr(self, key))
				return any(pattern.fullmatch(el) for el in collection)
			else:
				self_value = cast(str, getattr(self, key)).casefold()
				return self_value == value
		else:
			raise ValueError(f"Argument '{argname(value)}' has invalid type ({type(value)})")


class JournalList():
	_jdb: 'JournalDB'

	def __init__(self, jdb: 'JournalDB'):
		self._jdb = jdb

	def __repr__(self):
		return f"<{type(self).__name__} count={len(self)!r}>"

	def __iter__(self) -> Iterator[Tuple[JournalID, Journal]]:
		it = self._jdb._db.iteritems(self._jdb._journals_cf)
		it.seek_to_first()
		for (_, key), value in it:
			yield _unpack(key), _unpack(value, _decode)

	def __len__(self):
		it = self._jdb._db.iterkeys(self._jdb._journals_cf)
		it.seek_to_first()
		return sum(1 for _ in it)

	def sanitize_journal_name(self, name: str) -> str:
		import unicodedata

		name = _ignorable_words_regex.sub("", name)

		chars = ["\0"] * len(name)
		length = 0
		for char in name:
			if unicodedata.category(char)[0] in ("L", "N", "Z") or char == ":":
				chars[length] = char
				length += 1

		return "".join(islice(chars, length))

	def iter_name_index(self) -> Iterator[Tuple[str, JournalID]]:
		it = self._jdb._db.iteritems(self._jdb._journal_names_index_cf)
		it.seek_to_first()
		for (_, key), value in it:
			yield _unpack(key), _unpack(value, _decode)

	def get(self, id: JournalID) -> Optional[Journal]:
		id_bytes = _pack(id)
		if self._jdb._db.key_may_exist((self._jdb._journals_cf, id_bytes)):
			return _unpack(self._jdb._db.get((self._jdb._journals_cf, id_bytes)), _decode)
		else:
			return None

	def add(self, journal: Journal, batch: rocksdb.WriteBatch = None) -> JournalID:
		batch, new_batch = (rocksdb.WriteBatch(), True) if batch is None else (batch, False)
		id = self._jdb._gen_journal_id(batch)
		batch.put((self._jdb._journals_cf, _pack(id)), _pack(journal, _encode))
		for name in journal.names:
			index_name = self.sanitize_journal_name(name).casefold()
			batch.put((self._jdb._journal_names_index_cf, _pack(index_name)), _pack(id))
		if new_batch:
			self._jdb._db.write(batch)
		return id

	def remove(self, id: JournalID, batch: rocksdb.WriteBatch = None) -> Optional[bool]:
		journal = self.get(id)
		if journal is None:
			return False

		batch, new_batch = (rocksdb.WriteBatch(), True) if batch is None else (batch, False)
		batch.delete((self._jdb._journals_cf, _pack(id)))
		for name in journal.names:
			batch.delete((self._jdb._journal_names_index_cf, _pack(name)))
		if new_batch:
			try:
				self._jdb._db.write(batch)
			except rocksdb.NotFound:
				return False
			else:
				return True

		return True

	def update(self, id: JournalID, journal_new: Journal, journal_old: Optional[Journal] = None, batch: rocksdb.WriteBatch = None):
		if journal_old is None:
			journal_old = self.get(id)

		batch, new_batch = (rocksdb.WriteBatch(), True) if batch is None else (batch, False)
		batch.put((self._jdb._journals_cf, _pack(id)), _pack(journal_new, _encode))
		for name in journal_old.names - journal_new.names:
			batch.delete((self._jdb._journal_names_index_cf, _pack(name)))
		for name in journal_new.names - journal_old.names:
			batch.put((self._jdb._journal_names_index_cf, _pack(name)), _pack(id))
		if new_batch:
			self._jdb._db.write(batch)

	def merge(self, id: JournalID, journal_new: Journal, journal_old: Optional[Journal] = None, batch: rocksdb.WriteBatch = None):
		if journal_old is None:
			journal_old = self.get(id)

		journal_merged = Journal.merge(journal_old, journal_new)
		self.update(id, journal_merged, journal_old, batch)

	def query(self, match_key: str, match_value: Union[str, Pattern[str]]) -> Iterator[Tuple[JournalID, Journal]]:
		def name_to_id(name: str) -> Optional[JournalID]:
			index_name = self.sanitize_journal_name(name).casefold()
			id_bytes = self._jdb._db.get((self._jdb._journal_names_index_cf, _pack(index_name)))
			return JournalID(_unpack(id_bytes)) if id_bytes is not None else None

		def name_to_journal(name: str) -> Optional[Tuple[JournalID, Journal]]:
			id = name_to_id(name)
			journal = self.get(id)
			assert (id is None or journal is not None)
			return (id, journal) if id is not None else None

		if match_key == Journal.names_key:
			# Check with index of journal names can be used.
			names = None
			if isinstance(match_value, str):
				name = cast(str, match_value)
				if res := name_to_journal(name):
					return (res,)
			elif isinstance(match_value, Collection):
				names = cast(Collection[str], match_value)
				return filter(None, (name_to_journal(name) for name in names))
			elif isinstance(match_value, Pattern):
				pattern = cast(Pattern[str], match_value)
				return ((id, self.get(id)) for name, id in self.iter_name_index() if pattern.fullmatch(name))
				# return ((id, self.get(id)) for id, journal in iter(self) for name in journal.names if pattern.fullmatch(name))

		return ((id, journal) for id, journal in self if journal.matches(match_key, match_value))

	def query_one(self, match_key: str, match_value: Union[str, Pattern[str]]) -> Optional[Tuple[JournalID, Journal]]:
		return next(self.query(match_key, match_value), None)

	def reserialize(self):
		for id, journal in self:
			self._jdb._db.put((self._jdb._journals_cf, _pack(id)), _pack(journal, _encode))

	def delete_indexes(self):
		self._jdb._db.drop_column_family(self._jdb._journal_names_index_cf)

	def rebuild_indexes(self):
		self.delete_indexes()

		self._jdb._journal_names_index_cf = self._jdb._db.create_column_family(self._jdb._journal_names_index_cf_name, self._jdb._db_col_families[self._jdb._journal_names_index_cf_name])

		for id, journal in self:
			for name in journal.names:
				index_name = self.sanitize_journal_name(name).casefold()
				self._jdb._db.put((self._jdb._journal_names_index_cf, _pack(index_name)), _pack(id))


class StringComparator(Comparator):
	def name(self) -> bytes:
		return self.__class__.__name__.encode()

	def compare(self, a: bytes, b: bytes) -> int:
		return _cmp(_unpack(a), _unpack(b))


class HierarchicalComparator(Comparator):
	def name(self) -> bytes:
		return self.__class__.__name__.encode()

	def compare(self, a: bytes, b: bytes) -> int:
		a_parts = a.split(b":")
		b_parts = b.split(b":")
		for a_part, b_part in zip(a_parts, b_parts):
			a_b_cmp = _cmp(a_part, b_part)
			if a_b_cmp != 0:
				return a_b_cmp
		return _cmp(len(a_parts), len(b_parts))


class TopLevelPrefixExtractor(SliceTransform):
	def name(self) -> bytes:
		return self.__class__.__name__.encode()

	def transform(self, src: bytes) -> Tuple[int, int]:
		return (0, src.find(":"))

	def in_domain(self, src: bytes) -> bool:
		return ":" in src

	def in_range(self, dst: bytes) -> bool:
		return not ":" in dst


# class FooAssociativeMerger(AssociativeMergeOperator):
# 	def name(self) -> bytes:
# 		return self.__class__.__name__.encode()
#
# 	def merge(self, key: bytes, existing_value: Optional[bytes], value: bytes) -> Tuple[bool, bytes]:
# 		pass
#
# class FooMerger(MergeOperator):
# 	def name(self) -> bytes:
# 		return self.__class__.__name__.encode()
#
# 	def full_merge(self, key: bytes, existing_value: Optional[bytes], operand_list: List[bytes]) -> Tuple[bool, bytes]:
# 		pass
#
# 	def partial_merge(self, key: bytes, left_operand: bytes, right_operand: bytes) -> Tuple[bool, bytes]:
# 		pass


class MetadataMergeOperator(AssociativeMergeOperator):
	def name(self) -> bytes:
		return self.__class__.__name__.encode()

	def merge(self, key: bytes, existing_value: Optional[bytes], value: bytes) -> Tuple[bool, bytes]:
		if existing_value is not None:
			if key == JournalDB._journal_id_key:
				new_value = cast(int, _unpack(existing_value)) + cast(int, _unpack(value))
				return (True, _pack(new_value))
		return (True, value)


class JournalDB(EventEmitter):
	_metadata_cf_name: ClassVar[bytes] = b"metadata"
	_journals_cf_name: ClassVar[bytes] = b"journals"
	_journal_names_index_cf_name: ClassVar[bytes] = b"journal_names"
	_schema_version_key: ClassVar[bytes] = b"schema.version"
	_journal_id_key: ClassVar[bytes] = b"journal_id"
	latest_schema_version: ClassVar[Version] = Version(__version__)

	_filename: str
	_db: rocksdb.DB = None
	_metadata_cf: rocksdb.ColumnFamilyHandle = None
	_journals_cf: rocksdb.ColumnFamilyHandle = None
	_journal_names_index_cf: rocksdb.ColumnFamilyHandle = None

	def __init__(self, force_upgrade_schema = False):
		def column_family_options(comparator: Optional[Comparator] = None, prefix_extractor: Optional[SliceTransform] = None, merge_operator: Optional[Union[MergeOperator, AssociativeMergeOperator]] = None) -> rocksdb.ColumnFamilyOptions:
			opts = rocksdb.ColumnFamilyOptions()
			if comparator is not None:
				opts.comparator = comparator
			if prefix_extractor is not None:
				opts.prefix_extractor = prefix_extractor
			if merge_operator is not None:
				opts.merge_operator = merge_operator

			return opts

		super().__init__(self)

		self._filename = os.path.join(app_user_data_dir, "db.rocksdb")
		self._force_upgrade_schema = force_upgrade_schema

		self._db_opts = rocksdb.Options(
			create_if_missing = True,
			create_missing_column_families = True,
			# comparator = HierarchicalComparator(),
			# prefix_extractor = TopLevelPrefixExtractor(),
		)
		self._db_col_families = {
			self._metadata_cf_name: column_family_options(merge_operator = MetadataMergeOperator()),
			self._journals_cf_name: column_family_options(),
			self._journal_names_index_cf_name: column_family_options(comparator = StringComparator()),
		}

	def __repr__(self) -> str:
		return f"<{type(self).__name__} schema_version={self.schema_version()!r} journals={self.journals!r}>"

	def __enter__(self) -> 'JournalDB':
		self.open()
		return self

	def __exit__(self, exc_type, exc_value, traceback):
		self.close()
		return False

	@property
	def filename(self):
		return self._filename

	def _upgrade_schema(self, cur_schema_version: Version):
		self.emit("upgrade_started", cur_schema_version, self.latest_schema_version, len(self._journal_list))

		def update_journal(id: JournalID, journal: Journal) -> Optional[Journal]:
			if cur_schema_version <= Version("0.1.0"):
				pass
			return None

		self.emit("upgrade_progress", 0)

		# Upgrade each journal.
		num_processed = 0
		num_updated = 0
		for id, journal in self._journal_list:
			updated_journal = update_journal(id, journal)
			if update_journal is not None:
				self._journal_list.update(id, updated_journal)
				num_updated += 1

			num_processed += 1
			self.emit("upgrade_progress", num_processed, num_updated)

		self._pu_metadata(self._schema_version_key, str(self.latest_schema_version))

		self.emit("upgrade_finished", self.latest_schema_version, len(self._journal_list))

	def repair(self):
		self._db.repair_db(self._filename, self._db_opts)

	def open(self):
		db_exists = os.path.exists(self._filename)

		self._db = rocksdb.DB(self._filename, self._db_opts, column_families = self._db_col_families)
		self._metadata_cf = self._db.get_column_family(self._metadata_cf_name)
		self._journals_cf = self._db.get_column_family(self._journals_cf_name)
		self._journal_names_index_cf = self._db.get_column_family(self._journal_names_index_cf_name)

		self._journal_list = JournalList(self)

		if not db_exists:
			# Datbase was just created; initialize metadata.

			self._put_metadata(self._schema_version_key, str(self.latest_schema_version))
			self._put_metadata(self._journal_id_key, 0)

			self.emit("created", self.latest_schema_version)
		else:
			cur_schema_version_str = self._get_metadata(self._schema_version_key)
			cur_schema_version = Version(cur_schema_version_str) if cur_schema_version_str is not None else cur_schema_version_str

			if cur_schema_version < self.latest_schema_version or self._force_upgrade_schema:
				# Database has outdated schema; upgrade it.

				self._upgrade_schema(cur_schema_version)

	def close(self):
		if self._db is not None:
			self._db.close()
			self._db = None

	def _check_open(self) -> bool:
		return self._db is not None

	def _get_metadata(self, key: bytes, decode_fn: _DecodeFn = None) -> Optional[Any]:
		return _unpack(self._db.get((self._metadata_cf, key)), fn = decode_fn)

	def _put_metadata(self, key: bytes, value: Any, batch: rocksdb.WriteBatch = None, encode_fn: _EncodeFn = None):
		(batch or self._db).put((self._metadata_cf, key), _pack(value, encode_fn))

	def _merge_metadata(self, key: bytes, value: Any, batch: rocksdb.WriteBatch = None, encode_fn: _EncodeFn = None):
		(batch or self._db).merge((self._metadata_cf, key), _pack(value, encode_fn))

	def _gen_journal_id(self, batch: rocksdb.WriteBatch = None) -> JournalID:
		id = cast(int, self._get_metadata(self._journal_id_key))
		self._merge_metadata(self._journal_id_key, 1, batch)
		return JournalID(id)

	def schema_version(self) -> Optional[Version]:
		self._check_open()

		version_str = self._get_metadata(self._schema_version_key)
		return Version(version_str) if version_str is not None else None

	@property
	def journals(self) -> JournalList:
		self._check_open()

		return self._journal_list


def _cmp(x: T, y: T) -> int:
	"""
	Compare two objects and returns an integer.

	The return value is zero if `x == y`, positive if `x > y`, and negative if `x < y`.
	"""

	return (x > y) - (x < y)


def _encode(obj: Any) -> Any:
	if isinstance(obj, set):
		return msgpack.ExtType(1, msgpack.packb(tuple(obj), default = _encode))
	elif isinstance(obj, Journal):
		return msgpack.ExtType(10, msgpack.packb(obj.asdict(), default = _encode))
	raise TypeError(f"Unknown type {type(obj)}")


def _decode(code: int, data: Any) -> Any:
	if code == 1:
		return set(msgpack.unpackb(data, ext_hook = _decode))
	elif code == 10:
		return Journal.fromdict(msgpack.unpackb(data, ext_hook = _decode))
	return msgpack.ExtType(code, data)


def _pack(obj: Any, fn: _EncodeFn = None) -> bytes:
	return msgpack.packb(obj, default = fn)


def _unpack(data: bytes, fn: _DecodeFn = None) -> Any:
	return msgpack.unpackb(data, ext_hook = fn) if data is not None else None
