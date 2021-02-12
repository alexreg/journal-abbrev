from argparse import Action, ArgumentError, ArgumentParser, Namespace
from io import TextIOWrapper
import bibtexparser
from bibtexparser.bibdatabase import BibDatabase
from bibtexparser.bwriter import BibTexWriter
import os
import re
from re import RegexFlag
import sys
from textwrap import dedent
from tinydb import TinyDB
from tinydb.table import Table
from typing import *

from journalabbrev.common import *
from journalabbrev.db import *

from journalabbrev_cli.common import *
from journalabbrev_cli.db import *


if not TYPE_CHECKING:
	IO = Any

braces_regex = re.compile(r"(?<!\\)[{}]", RegexFlag.IGNORECASE)


def gen_sourcemap_map(journal_doc, journaltitle: str, abbrev: str, output_io: IO):
	output_io.write(
		dedent(rf"""
			\DeclareSourcemap{{
				\maps[datatype = bibtex]{{
					\map[overwrite, foreach={{journal, journaltitle}}]{{
						\step[fieldsource=\regexp{{$MAPLOOP}}, matchi={{{journaltitle}}}, replace={{{abbrev or journaltitle}}}]
					}}
				}}
			}}
		""").lstrip()
	)
	output_io.write("\n")


# TODO: allow using different abbreviation formats.
def proc_bib(input_io: TextIOWrapper, output_io: TextIOWrapper, db: JournalDB, silent: bool = False, output_format: str = "bib"):
	journals_list = db.journals()

	bib_db = bibtexparser.load(input_io)

	for cur_entry in bib_db.entries:
		journaltitle = cur_entry.get("journaltitle")
		if journaltitle is None:
			continue
		journaltitle = braces_regex.sub("", journaltitle)

		name_pattern = re.compile(fr"^{re.escape(journaltitle)}($|:)", RegexFlag.IGNORECASE)
		journal_doc = journals_list.get(Journal.name_key, name_pattern)
		abbrev = journal_doc.iso4 if journal_doc else None

		if output_format == "bib":
			cur_entry["journaltitle"] = f"{{{abbrev or journaltitle}}}"
		elif output_format == "sourcemap":
			gen_sourcemap_map(journal_doc, journaltitle, abbrev, output_io)

		abbrev_msg = f"abbreviating to '{abbrev}'" if abbrev is not None else f"no abbreviation found"
		if not silent:
			info(f"found journal name '{journaltitle}'; {abbrev_msg}.")

	if output_format == "bib":
		bib_writer = BibTexWriter()
		bib_writer.add_trailing_comma = True
		bib_writer.display_order = None
		bib_writer.indent = "\t"
		bib_writer.order_entries_by = None
		bibtex_code = bib_writer.write(bib_db)
		output_io.write(bibtex_code)
	elif output_format == "sourcemap":
		pass


def cmd_proc_bibs(db: TinyDB, args: Namespace, *, output_format_arg: Action, output_filenames_arg: Action):
	def get_output_filename(filename):
		basename, ext = os.path.splitext(filename)
		if args.output_format == "bib":
			return basename + "-abbrev" + ext
		elif args.output_format == "sourcemap":
			return basename + "-abbrev-sourcemap.tex"
		else:
			raise ArgumentError(output_format_arg)

	filenames = args.filenames
	output_filenames = args.output_filenames or [get_output_filename(f) for f in filenames]
	if len(output_filenames) != len(filenames):
		raise ArgumentError(output_filenames_arg, f"expected {len(filenames)} filename(s); got {len(output_filenames)}")

	if len(filenames) == 0:
		if not args.silent:
			info(f"processing bib from stdin...")

		proc_bib(sys.stdin, sys.stdout, db, args.silent, args.output_format)

		if not args.silent:
			info(f"done processing stdin; written to stdout.")
	else:
		for cur_filename, cur_output_filename in zip(filenames, output_filenames):
			cur_input_io = None
			cur_output_io = None
			try:
				if not args.silent:
					info(f"processing bib file `{cur_filename}`...")

				cur_input_io = open(cur_filename, "r")
				if cur_output_filename == "-":
					cur_output_io = sys.stdout
				else:
					cur_output_io = open(cur_output_filename, "w")

				proc_bib(cur_input_io, cur_output_io, db, args.silent, args.output_format)

				if not args.silent:
					info(f"done processing bib file; written `{cur_output_filename}`.")
			finally:
				if cur_input_io is not None:
					cur_input_io.close()
				if cur_output_io is not None:
					cur_output_io.close()

	if not args.silent:
		info(f"all done.")


def main():
	default_arg_help = "(default: %(default)s)"

	parser = ArgumentParser(description = f"Abbreviates journal names in bibliography files.")
	parser.add_argument("filenames", metavar = "FILE", type = str, nargs = "*", help = f"a path to a bibliography (`.bib`) file to read")
	parser.add_argument("--silent", "-s", action = "store_true", help = f"do not write messages to stderr")
	output_format_arg = parser.add_argument("--output-format", "-f", type = str, choices = ["bib", "sourcemap"], default = "bib", help = f"the format of the output to generate {default_arg_help}")
	output_filenames_arg = parser.add_argument("--output-filenames", "-o", metavar = "OUTPUT_FILE", type = str, nargs = "+", help = f"the path to the bibliography (`.bib`) file to write, one per input file")
	args = parser.parse_args()

	try:
		with JournalDB() as db:
			handle_upgrade_events(db)
			db.open()

			cmd_proc_bibs(db, args,
				output_format_arg = output_format_arg,
				output_filenames_arg = output_filenames_arg,
			)
	except FatalError as e:
		sys.exit(e.exit_status)
	except Exception as e:
		if int(os.getenv("DEBUG", 0)):
			raise
		else:
			print(f"fatal error: {e}", file = sys.stdout)

if __name__ == "__main__":
    main()
