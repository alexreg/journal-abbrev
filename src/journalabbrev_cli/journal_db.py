from argparse import Action, ArgumentError, ArgumentParser, Namespace
import inspect
import io
import itertools
import json
import os
from progressbar import ProgressBar
import re
from re import M, RegexFlag
import signal
import sys
from typing import *

from journalabbrev.common import *
from journalabbrev.fetcher import *
from journalabbrev.db import *

from journalabbrev_cli.common import *
from journalabbrev_cli.db import *


is_canceling = False
fetchers = []

fetch_sources_map: Dict[str, Fetcher] = {
	"cas": CasFetcher,
	"beyond-cassi": BeyondCassiFetcher,
	"ubc": UbcFetcher,
	"mdpi": MdpiFetcher,
}

trailing_brackets_regex = re.compile(r" \(.+?\)$", RegexFlag.IGNORECASE)
no_space_after_dot_regex = re.compile(r"\.(\w)", RegexFlag(0)) # group is for upper-case character


def read_journals_stdin():
	buffered_stdin = cast(io.BufferedReader, sys.stdin.buffer)
	if len(buffered_stdin.peek(1)) == 0:
		return None
	json_input = json.load(buffered_stdin)
	if not isinstance(json_input, list):
		return [json_input]

def get_journal_queries(args: Namespace) -> Iterator[Tuple[str, Union[str, Pattern]]]:
	if len(args.journals) > 0:
		for name in args.journals:
			pattern = re.compile(fr"^{name}$", RegexFlag.IGNORECASE)
			yield Journal.name_key, pattern
	else:
		journals_json = read_journals_stdin()

		for cur_journal_json in journals_json:
			cur_journal_json = cast(dict, cur_journal_json)
			key = cast(str, cur_journal_json.get("key", Journal.name_key))
			value = cast(str, cur_journal_json.get("value"))
			is_regex = cast(bool, cur_journal_json.get("regex", True))

			if value is None:
				warn(f"entry has no 'value' key; ignoring: {cur_journal_json}")
				continue

			if is_regex:
				query_value = re.compile(fr"^{value}$", RegexFlag.IGNORECASE)
			else:
				query_value = value

			yield key, query_value

def fetch_source(name: str, db: JournalDB, overwrite: bool = False) -> bool:
	global is_canceling

	journals_list = db.journals()

	name = name.lower()
	cur_fetcher_ty = fetch_sources_map.get(name)
	if cur_fetcher_ty is None:
		error(f"unrecognised fetch source '{name}'.")
		raise FatalError()

	cur_fetcher = cur_fetcher_ty()
	fetchers.append(cur_fetcher)
	info(f"fetching source '{name}'...")
	journals = list(cur_fetcher.fetch())
	info(f"found {len(journals):,} journals in source '{name}'.")
	fetchers.remove(cur_fetcher)

	info(f"adding journals to database...")
	pbar = ProgressBar(max_value = len(journals))
	pbar.widgets = progressbar_count_widgets(pbar)
	pbar.start()

	num_journals_processed = 0
	num_journals_added = 0
	num_journals_updated = 0

	for cur_journal in journals:
		if is_canceling:
			break

		# Sanitize journal information.
		cur_journal.iso4 = sub_or_none(no_space_after_dot_regex, lambda m: (". " if str.isupper(m.group(1)) else ".") + m.group(1), cur_journal.iso4)

		if not journals_list.contains(Journal.name_key, cur_journal.name):
			journals_list.add(cur_journal)
			num_journals_added += 1
		else:
			try:
				journals_list.merge(cur_journal, overwrite)
				num_journals_updated += 1
			except InvalidMerge as e:
				print("\r", end = "", file = sys.stderr)

				_, attr, base, nxt = e.merge_args
				attr = ", ".join(attr)
				warn(f"failed merge for journal '{cur_journal.name}': key '{attr}' has base value `{base}` but new value `{nxt}`")

		num_journals_processed += 1
		pbar.update(num_journals_processed)

	pbar.finish()

	db.flush()

	info(f"added {num_journals_added:,} journal(s) to database; updated {num_journals_updated:,} journal(s) in database.")
	return True


def cmd_stats(db: JournalDB, args: Namespace):
	info(f"database format version: {db.format_version()}")
	info(f"total # of journals: {len(db.journals()):,}")


def cmd_add_journals(db: JournalDB, args: Namespace):
	journals_list = db.journals()

	journals_json = read_journals_stdin()

	def sanitize_value(f):
		f = f.strip()
		if len(f) > 0:
			return f
		else:
			return None

	for cur_journal_json in journals_json:
		for key, value in cur_journal_json.items():
			cur_journal_json[key] = sanitize_value(cur_journal_json[key])
		cur_journal = Journal(cur_journal_json)

		if cur_journal.name is None:
			warn(f"journal has no 'name' key; not adding: {cur_journal_json}")
			continue

		if not journals_list.contains(Journal.name_key, cur_journal.name):
			doc_id = journals_list.add(cur_journal)
			info(f"added journal #{doc_id} to database: {cur_journal.asdict()}")
		else:
			doc_ids = journals_list.update(cur_journal)
			info(f"updated journal #{doc_ids[0]} in database: {cur_journal.asdict()}")


def cmd_remove_journals(db: JournalDB, args: Namespace):
	journal_list = db.journals()

	JournalQuery = Query()

	doc_ids = list(itertools.chain.from_iterable(journal_list.remove(key, value) for key, value in get_journal_queries(args)))

	if len(doc_ids) > 0:
		doc_ids_str = ", ".join(f"#{id}" for id in doc_ids)
		info(f"removed journal(s) {doc_ids_str} from database")
	else:
		warn(f"did not find any matching journals")


def cmd_get_journals(db: JournalDB, args: Namespace):
	journal_list = db.journals()

	docs = list(itertools.chain.from_iterable(journal_list.get_all(key, value) for key, value in get_journal_queries(args)))

	if len(docs) > 0:
		info(f"found {len(docs)} matching journal(s) in database")
		for cur_doc in docs:
			info(f"journal: {cur_doc.asdict()}")
	else:
		warn(f"did not find any matching journals")


def cmd_list_fetch_sources(db: JournalDB, args: Namespace):
	source_name_regex = re.compile("^Source: (.*?)$")

	def get_source_name(cls):
		docstring = inspect.getdoc(cls)
		name = None
		for cur_line in docstring.split("\n"):
			match = source_name_regex.search(cur_line, flags = RegexFlag.MULTILINE)
			if match:
				name = match.group(1)
				break
		return name

	for id, cls in fetch_sources_map.items():
		url_msg_part = f" [{cls.url}]" if args.show_url and cls.url is not None else ""
		info(f"{id}: {get_source_name(cls) or '?'}" + url_msg_part)


def cmd_fetch_sources(db: JournalDB, args: Namespace):
	journals_list = db.journals()
	sources = args.sources

	if args.overwrite:
		warn(f"any existing journals will be overwritten.")

	sources_str = ", ".join(f"'{s}'" for s in sources)
	info(f"fetching {len(sources)} source(s): {sources_str}")

	for cur_source in sources:
		fetch_source(cur_source, db, args.overwrite)

	info(f"all done: {len(journals_list):,} journal(s) in total.")


def signal_handler(signum, frame):
	global is_canceling

	is_canceling = True
	if signum == signal.SIGINT:
		is_canceling = True
		for cur_fetcher in fetchers:
			cur_fetcher.cancel()


def main():
	signal.signal(signal.SIGINT, signal_handler)

	parser = ArgumentParser(description = f"Generates the database of journal abbreviations.")
	parser.set_defaults(subcommand = None)

	subparsers = parser.add_subparsers(help = f"subcommands")

	cmd_stats_parser = subparsers.add_parser("stats", help = f"show database statistics")
	cmd_stats_parser.set_defaults(subcommand = cmd_stats)

	cmd_add_parser = subparsers.add_parser("add", help = f"adds journal(s) to the database; takes JSON as input")
	cmd_add_parser.set_defaults(subcommand = cmd_add_journals)

	cmd_remove_parser = subparsers.add_parser("rem", help = f"removes journal(s) from the database; optionally takes JSON as input")
	cmd_remove_parser.set_defaults(subcommand = cmd_remove_journals)
	cmd_remove_parser.add_argument("journals", metavar = "NAME", type = str, nargs = "*", help = f"the journals to remove (regexes for full name)")

	cmd_get_parser = subparsers.add_parser("get", help = f"gets entres for journal(s) from the database; optionally takes JSON as input")
	cmd_get_parser.set_defaults(subcommand = cmd_get_journals)
	cmd_get_parser.add_argument("journals", metavar = "NAME", type = str, nargs = "*", help = f"the journals to get (regexes for full name)")

	cmd_list_fetch_sources_parser = subparsers.add_parser("listsources", help = f"lists fetch sources")
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
		with JournalDB() as db:
			handle_upgrade_events(db)
			db.open()

			args.subcommand(db, args)
	except FatalError as e:
		sys.exit(e.exit_status)
	except Exception as e:
		if int(os.getenv("DEBUG", 0)):
			raise
		else:
			print(f"fatal error: {e}", file = sys.stdout)

if __name__ == "__main__":
    main()
