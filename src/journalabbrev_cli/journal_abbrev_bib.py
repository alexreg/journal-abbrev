import os
import re
import sys
from argparse import Action, ArgumentError, ArgumentParser, Namespace
from functools import cache
from io import StringIO, TextIOWrapper
from re import RegexFlag
from textwrap import dedent
from typing import *

import bibtexparser
from bibtexparser.bibdatabase import BibDatabase
from bibtexparser.bwriter import BibTexWriter
from journalabbrev.common import *
from journalabbrev.db import *

from journalabbrev_cli.common import *
from journalabbrev_cli.db import *

if not TYPE_CHECKING:
	IO = Any

expand_latex_regex = re.compile(normalize_regex(r"""
	(?:{|}|\\(?P<macro>\w+)|\\(?P<char>\S))
"""))
journal_name_regex = re.compile(normalize_regex(r"""
	(?P<title>.+?)
	(?:\s(?:[Ss]eries )?(?P<series>[A-Z])\b)?
	(?:\:\s(?P<subtitle>.+)?)?
"""))


@cache
def expand_latex(s: str) -> str:
	def repl(m: Match[str]) -> str:
		macro = m.group("macro")
		if macro is not None:
			raise ValueError(f"Unrecognised macro `\{macro}` in LaTeX string.")
		
		char = m.group("char")
		if char is not None:
			return char
		
		return None

	return expand_latex_regex.sub(repl, s)


@cache
def find_journal(jdb: JournalDB, name: str):
	match = journal_name_regex.fullmatch(name)
	assert match is not None

	index_name = jdb.journals.get_journal_index_name(match.group("title"))

	pattern = StringIO()
	pattern.write(re.escape(index_name))
	if series := match.group("series"):
		pattern.write(fr" (series )?")
		pattern.write(series.casefold())
	if subtitle := match.group("subtitle"):
		pass

	name_regex = re.compile(fr"{pattern.getvalue()}(?:\:\s.*)?")
	return jdb.journals.query_one(Journal.names_key, name_regex)


def gen_sourcemap_map(output_io: IO, journal: Journal, journaltitle: str, abbrev: str, issn: str):
	new_journaltitle = abbrev or journaltitle
	issn_step_code = f"\step[fieldset = issn, fieldvalue = {{{issn}}}]" if issn else ""

	write_tex_code(output_io, rf"""
			\DeclareSourcemap{{
				\maps[datatype = bibtex]{{
					\map[overwrite, foreach = {{journal, journaltitle}}]{{
						\step[fieldsource = \regexp{{$MAPLOOP}}, matchi = {{^{re.escape(journaltitle)}$}}, final]
						\step[fieldset = \regexp{{$MAPLOOP}}, fieldvalue = {{{{{new_journaltitle}}}}}]
						{issn_step_code}
					}}
				}}
			}}
		""")
	output_io.write("\n")


def write_tex_code(output_io: IO, code: str):
	lines = dedent(code).splitlines()
	non_empty_lines = (l + "\n" for l in lines if l)
	output_io.writelines(non_empty_lines)


def proc_bib(input_io: TextIOWrapper, output_io: TextIOWrapper, jdb: JournalDB, silent: bool = False, output_format: str = "bib", abbrev_type = "iso4"):
	if not hasattr(Journal, abbrev_type):
		raise ValueError(f"Invalid abbreviation type `{abbrev_type}`")

	bib_db = bibtexparser.load(input_io)

	for entry in bib_db.entries:
		journaltitle = entry.get("journaltitle")
		if journaltitle is None:
			continue
		journaltitle = expand_latex(journaltitle)

		# TODO: Use Levenstein distance or similar?
		res = find_journal(jdb, journaltitle)
		if res:
			_, journal = res
			abbrev = getattr(journal, abbrev_type)
			issn = ", ".join(filter(None, (journal.issn_web, journal.issn_print)))

			if output_format == "bib":
				entry["journaltitle"] = f"{{{abbrev or journaltitle}}}"
				entry["issn"] = entry["issn"] or f"{{{issn}}}"
			elif output_format == "sourcemap":
				gen_sourcemap_map(output_io, journal, journaltitle, abbrev, issn)

		abbrev_msg = f"abbreviating to '{abbrev}'" if res else f"no abbreviation found"
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


def cmd_proc_bibs(jdb: JournalDB, args: Namespace, *, output_format_arg: Action, output_filenames_arg: Action):
	def get_output_filename(filename):
		basename, ext = os.path.splitext(filename)
		if args.output_format == "bib":
			return basename + "-abbrev" + ext
		elif args.output_format == "sourcemap":
			return basename + "-abbrev-sourcemap.tex"
		else:
			raise ArgumentError(output_format_arg)

	filenames = args.filenames
	output_filenames = args.output_filenames or [get_output_filename(filename) for filename in filenames]
	if len(output_filenames) != len(filenames):
		raise ArgumentError(output_filenames_arg, f"Expected {len(filenames)} filename(s); got {len(output_filenames)}")

	if not filenames:
		if not args.silent:
			info(f"processing bib from stdin...")

		proc_bib(sys.stdin, sys.stdout, jdb, args.silent, args.output_format, args.abbrev_type)

		if not args.silent:
			info(f"done processing stdin; written to stdout.")
	else:
		for filename, output_filename in zip(filenames, output_filenames):
			input_io = None
			output_io = None
			try:
				if not args.silent:
					info(f"processing bib file `{filename}`...")

				input_io = open(filename, "r")
				if output_filename == "-":
					output_io = sys.stdout
				else:
					output_io = open(output_filename, "w")

				proc_bib(input_io, output_io, jdb, args.silent, args.output_format, args.abbrev_type)

				if not args.silent:
					info(f"done processing bib file; written `{output_filename}`.")
			finally:
				if input_io is not None:
					input_io.close()
				if output_io is not None:
					output_io.close()

	if not args.silent:
		info(f"all done.")


def main():
	default_arg_help = "(default: %(default)s)"

	parser = ArgumentParser(description = f"Abbreviates journal names in bibliography files.")
	parser.add_argument("filenames", metavar = "FILE", type = str, nargs = "*", help = f"a path to a bibliography (`.bib`) file to read")
	parser.add_argument("--silent", "-s", action = "store_true", help = f"do not write messages to stderr")
	parser.add_argument("--online", "-n", action = "store_true", help = f"query online databases if no match is found in the local database")
	parser.add_argument("--abbrev-type", "-t", type = str, choices = ["iso4", "coden"], default = "iso4", help = f"the type of abbreviation to output {default_arg_help}")
	output_format_arg = parser.add_argument("--output-format", "-f", type = str, choices = ["bib", "sourcemap"], default = "bib", help = f"the format of the output to generate {default_arg_help}")
	output_filenames_arg = parser.add_argument("--output-filenames", "-o", metavar = "OUTPUT_FILE", type = str, nargs = "+", help = f"the path to the bibliography (`.bib`) file to write, one per input file")
	args = parser.parse_args()

	try:
		with JournalDB() as jdb:
			cmd_proc_bibs(
				jdb,
				args,
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
