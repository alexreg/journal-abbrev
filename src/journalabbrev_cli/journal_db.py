from argparse import Action, ArgumentError, ArgumentParser, Namespace
import inspect
import io
from itertools import *
import json5
import os
from progressbar import ProgressBar
import re
from re import M, RegexFlag
import signal
import sys
from typing import *

from journalabbrev.common import *
from journalabbrev.db import *
from journalabbrev.fetcher import *

from journalabbrev_cli.common import *
from journalabbrev_cli.db import *


JournalQuery = Tuple[str, Union[str, Pattern[str]]]


is_canceling = False
fetchers = []

fetcher_map: Dict[str, Fetcher]

trailing_brackets_regex = re.compile(r" \(.+?\)$", RegexFlag.IGNORECASE)
no_space_after_dot_regex = re.compile(r"\.(\w)") # group is for upper-case character


def find_fetch_sources():
	import journalabbrev.fetcher

	global fetcher_map

	def normalize_name(name: str) -> str:
		return name.casefold().replace(" ", "-")

	name_attr = "name"
	fetcher_classes = (typ for name, typ in inspect.getmembers(journalabbrev.fetcher) if inspect.isclass(typ) and issubclass(typ, Fetcher) and hasattr(typ, name_attr))
	fetcher_map = {normalize_name(getattr(typ, name_attr)): typ for typ in fetcher_classes}


def read_journals_stdin():
	def object_hook(dct):
		if "names" in dct:
			dct["names"] = set(dct["names"])
		return dct

	buffered_stdin = cast(io.BufferedReader, sys.stdin.buffer)
	if not buffered_stdin.peek(1):
		return None
	json_input = json5.load(buffered_stdin, object_hook = object_hook)
	if not isinstance(json_input, list):
		return [json_input]
	return json_input


def get_journal_queries(args: Namespace) -> Iterator[Union[JournalID, JournalQuery]]:
	if args.journals:
		for journal in args.journals:
			if (id := try_int(journal)) is not None:
				yield JournalID(id)
				continue

			name = cast(str, journal)
			pattern = re.compile(fr"^{name}$", RegexFlag.IGNORECASE)
			yield Journal.names_key, pattern
	else:
		journal_list_json = sanitize_json(read_journals_stdin())

		for journal_json in journal_list_json:
			journal_json = cast(dict, journal_json)

			id = cast(Optional[str], journal_json.get("id", None))
			if id is not None:
				yield JournalID(id)
				continue

			key = cast(str, journal_json.get("key", Journal.names_key))
			value = cast(str, journal_json.get("value"))
			is_regex = cast(bool, journal_json.get("regex", True))

			if value is None:
				warn(f"entry has no 'value' key; ignoring: {journal_json}")
				continue

			if is_regex:
				query_value = re.compile(fr"^{value}$", RegexFlag.IGNORECASE)
			else:
				query_value = value

			yield key, query_value

def fetch_source(name: str, jdb: JournalDB, overwrite: bool = False) -> bool:
	global is_canceling

	name = name.casefold()
	fetcher_typ = fetcher_map.get(name)
	if fetcher_typ is None:
		error(f"unrecognised fetch source '{name}'")
		raise FatalError()

	fetcher = fetcher_typ()
	fetchers.append(fetcher)
	info(f"fetching source '{name}'...")
	journals = list(fetcher.fetch())
	info(f"found {len(journals):,} journals in source '{name}'")
	fetchers.remove(fetcher)

	info(f"adding journals to database...")
	pbar = ProgressBar(max_value = len(journals))
	pbar.widgets = progressbar_count_widgets(pbar)
	pbar.start()

	num_journals_processed = 0
	num_journals_added = 0
	num_journals_updated = 0
	num_warnings = 0

	for journal in journals:
		if is_canceling:
			break

		# Sanitize journal information.
		journal.iso4 = sub_or_none(no_space_after_dot_regex, lambda m: (". " if str.isupper(m.group(1)) else ".") + m.group(1), journal.iso4)

		found_journals = list(jdb.journals.query(Journal.names_key, journal.names))
		if len(found_journals) == 0:
			jdb.journals.add(journal)
			num_journals_added += 1
		elif len(found_journals) == 1:
			try:
				id, journal_old = found_journals[0]
				if overwrite:
					jdb.journals.update(id, journal, journal_old)
				else:
					jdb.journals.merge(id, journal, journal_old)
			except InvalidMerge as e:
				print("\r", end = "", file = sys.stderr)

				id, journal = found_journals[0]
				journal_names = ", ".join(journal.names)

				_, attr, base, nxt = e.merge_args
				attr = ", ".join(attr)
				warn(f"failed merge for journal #{id} ({journal_names}): key '{attr}' has base value `{base}` but new value `{nxt}`")
				num_warnings += 1
			else:
				num_journals_updated += 1
		else:
			journal_names_str = ", ".join(journal.names)
			warn("multiple journals in database with names matching `{journal_names_str}`; not merging")
			num_warnings += 1

		num_journals_processed += 1
		pbar.update(num_journals_processed)

	pbar.finish()

	info(f"added {num_journals_added:,} journal(s) to database; updated {num_journals_updated:,} journal(s) in database; {num_warnings:,} warning(s)")
	return True


def cmd_reserialize(jdb: JournalDB, args: Namespace):
	info(f"reserialising database entries...")
	jdb.journals.reserialize()
	info(f"done")


def cmd_delete_indexes(jdb: JournalDB, args: Namespace):
	info(f"deleting database indexes...")
	jdb.journals.delete_indexes()
	info(f"done")


def cmd_rebuild_indexes(jdb: JournalDB, args: Namespace):
	info(f"rebuilding database indexes...")
	jdb.journals.rebuild_indexes()
	info(f"done")


def cmd_info(jdb: JournalDB, args: Namespace):
	info(f"db filename: {jdb.filename}")
	info(f"database schema version: {jdb.schema_version()}")
	info(f"total # of journals: {len(jdb.journals):,}")
	info(f"total # of indexed journal names: {sum(1 for _ in jdb.journals.iter_name_index()):,}")


def cmd_add_journals(jdb: JournalDB, args: Namespace):
	journal_list_json = sanitize_json(read_journals_stdin())

	if journal_list_json is None:
		warn("no input")
		return

	for journal_json in journal_list_json:
		journal = Journal.fromdict(journal_json)

		if not journal.names:
			warn(f"journal has no '{Journal.names_key}' key; not adding: {journal_json}")
			continue

		found_journals = list(jdb.journals.query(Journal.names_key, journal.names))
		if len(found_journals) == 0:
			id = jdb.journals.add(journal)
			info(f"added journal #{id} to database: {journal.asdict()}")
		elif len(found_journals) == 1:
			id, journal_old = found_journals[0]
			if args.overwrite:
				jdb.journals.update(id, journal, journal_old)
				info(f"replaced journal #{id} in database: {journal.asdict()}")
			else:
				jdb.journals.merge(id, journal, journal_old)
				info(f"merged with journal #{id} in database: {journal.asdict()}")
		else:
			journal_names_str = ", ".join(journal.names)
			error("multiple journals in database with names matching `{journal_names_str}`; not merging")


def cmd_remove_journals(jdb: JournalDB, args: Namespace):
	removed_ids = []
	for query in get_journal_queries(args):
		if isinstance(query, JournalID.__supertype__):
			id = cast(JournalID, query)
			if jdb.journals.remove(id):
				removed_ids.append(id)
		else:
			key, value = cast(JournalQuery, query)
			for id, _ in jdb.journals.query(key, value):
				if jdb.journals.remove(id):
					removed_ids.append(id)

	if removed_ids:
		removed_ids_str = ", ".join(f"#{id}" for id in removed_ids)
		info(f"removed journal(s) {removed_ids_str} from database")
	else:
		warn(f"did not find any matching journals")


def cmd_get_journals(jdb: JournalDB, args: Namespace):
	journals = []
	for query in get_journal_queries(args):
		if isinstance(query, JournalID.__supertype__):
			id = cast(JournalID, query)
			journal = jdb.journals.get(id)
			if journal is not None:
				journals.append((id, journal))
		else:
			key, value = cast(JournalQuery, query)
			for id, journal in jdb.journals.query(key, value):
				journals.append((id, journal))

	if journals:
		info(f"found {len(journals)} matching journal(s) in database")
		for id, journal in journals:
			info(f"journal #{id:,}: {journal.asdict()}")
	else:
		warn(f"did not find any matching journals")


def cmd_list_fetch_sources(jdb: JournalDB, args: Namespace):
	source_name_regex = re.compile("^Source: (.*?)$", flags = RegexFlag.MULTILINE)

	def get_source_name(cls):
		docstring = inspect.getdoc(cls)
		match = source_name_regex.search(docstring)
		return match.group(1) if match else None

	for id, cls in fetcher_map.items():
		url_msg_part = f" [{cls.url}]" if args.show_url and cls.url is not None else ""
		info(f"{id}: {get_source_name(cls) or '?'}" + url_msg_part)


def cmd_fetch_sources(jdb: JournalDB, args: Namespace):
	sources = args.sources

	if args.overwrite:
		warn(f"any existing journals will be overwritten")

	sources_str = ", ".join(f"'{source}'" for source in sources)
	info(f"fetching {len(sources)} source(s): {sources_str}")

	for source in sources:
		fetch_source(source, jdb, args.overwrite)

	info(f"all done: {len(jdb.journals):,} journal(s) in total")


def signal_handler(signum, frame):
	global is_canceling

	is_canceling = True
	if signum == signal.SIGINT:
		is_canceling = True
		for fetcher in fetchers:
			fetcher.cancel()


def main():
	signal.signal(signal.SIGINT, signal_handler)

	find_fetch_sources()

	parser = ArgumentParser(description = f"Generates the database of journal abbreviations.")
	parser.set_defaults(subcommand = None)
	parser.add_argument("--force-upgrade-schema", "-u", action = "store_true", help = f"force upgrading the database schema")

	subparsers = parser.add_subparsers(help = f"subcommands")

	cmd_reserialize_parser = subparsers.add_parser("reserialize", help = f"reserialize all entries in database")
	cmd_reserialize_parser.set_defaults(subcommand = cmd_reserialize)

	cmd_delete_indexes_parser = subparsers.add_parser("delete-indexes", help = f"delete database indexes")
	cmd_delete_indexes_parser.set_defaults(subcommand = cmd_delete_indexes)

	cmd_rebuild_indexes_parser = subparsers.add_parser("rebuild-indexes", help = f"rebuild database indexes")
	cmd_rebuild_indexes_parser.set_defaults(subcommand = cmd_rebuild_indexes)

	cmd_info_parser = subparsers.add_parser("info", help = f"show database statistics")
	cmd_info_parser.set_defaults(subcommand = cmd_info)

	cmd_add_parser = subparsers.add_parser("add", help = f"adds journal(s) to the database; takes JSON as input")
	cmd_add_parser.set_defaults(subcommand = cmd_add_journals)
	cmd_add_parser.add_argument("--overwrite", "-o", action = "store_true", help = f"overwrite pre-existing entry if one exists")

	cmd_remove_parser = subparsers.add_parser("remove", help = f"removes journal(s) from the database; optionally takes JSON as input")
	cmd_remove_parser.set_defaults(subcommand = cmd_remove_journals)
	cmd_remove_parser.add_argument("journals", metavar = "ID | NAME", type = str, nargs = "*", help = f"the journals to remove (IDs or regexes for full name)")

	cmd_get_parser = subparsers.add_parser("get", help = f"gets entres for journal(s) from the database; optionally takes JSON as input")
	cmd_get_parser.set_defaults(subcommand = cmd_get_journals)
	cmd_get_parser.add_argument("journals", metavar = "ID | NAME", type = str, nargs = "*", help = f"the journals to get (IDs or regexes for full name)")

	cmd_list_fetch_sources_parser = subparsers.add_parser("list-sources", help = f"lists fetch sources")
	cmd_list_fetch_sources_parser.add_argument("--url", "-u", dest = "show_url", action = "store_true", help = f"show URLs of sources")
	cmd_list_fetch_sources_parser.set_defaults(subcommand = cmd_list_fetch_sources)

	cmd_fetch_parser = subparsers.add_parser("fetch", help = f"fetches source(s) to add to the database")
	cmd_fetch_parser.set_defaults(subcommand = cmd_fetch_sources)
	cmd_fetch_parser.add_argument("sources", metavar = "SOURCE", type = str, nargs = "+", help = f"the sources to fetch")
	cmd_fetch_parser.add_argument("--overwrite", "-o", action = "store_true", help = f"overwrite pre-existing entries")

	args = parser.parse_args()
	if args.subcommand is None:
		parser.error("a subcommand must be specified")

	try:
		with JournalDB(args.force_upgrade_schema) as jdb:
			args.subcommand(jdb, args)
	except FatalError as e:
		sys.exit(e.exit_status)
	except Exception as e:
		if int(os.getenv("DEBUG", 0)):
			raise
		else:
			print(f"fatal error: {e}", file = sys.stdout)


if __name__ == "__main__":
    main()
