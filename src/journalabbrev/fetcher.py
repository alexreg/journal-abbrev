import json
import re
from abc import *
from decimal import *
from io import StringIO
from itertools import *
from re import RegexFlag

import pdfplumber
import requests
from bs4 import BeautifulSoup
from bs4.element import PageElement
from more_itertools import peekable, windowed
from more_itertools.more import replace
from pdfminer.converter import PDFPageAggregator
from pdfminer.pdfinterp import PDFPageInterpreter

from .common import *
from .db import *
from .pdf import *


PDFChar = Dict


class Fetcher:
	name: ClassVar[str]

	def __init__(self):
		self._is_canceling = False

	@abstractmethod
	def fetch(self) -> Iterator[Journal]:
		pass

	def cancel(self):
		self._is_canceling = True


class CasFetcher(Fetcher):
	"""
	Source: Chemical Abstracts Service, Core Journals (American Chemical Society)
	"""

	name = "CAS"
	url = r"http://www.cas.org/support/documentation/references/corejournals"

	def __init__(self):
		super().__init__()

	def fetch(self) -> Iterator[Journal]:
		resp = requests.get(self.url)
		resp.raise_for_status()

		html = BeautifulSoup(resp.text, "html.parser")

		table = html.find("table")
		rows = table.find_all("tr")
		rows_iter = peekable(rows)
		first_row = next(rows_iter)
		if first_row.find_all("td")[0].text != "Publication Title":
			raise ProcessingError(f"Unexpected first row of table")

		while not self._is_canceling:
			row = next(rows_iter, None)
			if row is None:
				break

			cells = row.find_all("td")
			lines = (line.strip() for line in cells[0].text.split("\n", maxsplit = 1))
			lines = list(line.replace("\n", " ") for line in lines if len(line))

			journal = Journal()
			journal.coden = cast(str, cells[1].text).strip().upper()
			if len(lines) == 2:
				abbrev, name = lines
				journal.names.add(name)
				journal.iso4 = abbrev
			elif len(lines) == 1:
				abbrev = lines[0]
				name_lines = []
				while rows_iter and len(rows_iter.peek().find_all("td")) == 1:
					row = next(rows_iter)
					name_lines.append(row.find("td").text.strip())

				if name_lines:
					name = " ".join(name_lines)
				else:
					name = abbrev

				journal.iso4 = abbrev
				journal.names.add(name)
			else:
				raise ProcessingError(f"Unexpected number of lines in cell ({len(lines)})")

			yield journal

		return
		yield


class BeyondCassiFetcher(Fetcher):
	"""
	Source: Beyond CASSI (American Chemical Society)
	"""

	name = "Beyond CASSI"
	url = r"https://www.cas.org/sites/default/files/documents/beyond_cassi.pdf"

	header_text_regex = re.compile(r"JOURNAL TITLES\b.*")
	note_text_regex = re.compile(r"Note: *(.+?)")
	cassi_text_regex = re.compile(r"CASSI: *(.+?)(?:(?:\.\.\.|…)?\s*\[Note:\s*(.+?)\])?")
	url_regex = re.compile(r"(\w+)://(.+)")

	def __init__(self):
		super().__init__()

	def fetch(self) -> Iterator[Journal]:
		resp = requests.get(self.url)
		resp.raise_for_status()

		content_length = resp.headers.get("Content-Length", None)
		content_length = int(content_length) if content_length is not None else None
		cached_file = cache_in_memory(resp, size = content_length)
		with pdfplumber.open(cached_file) as pdf:
			def get_entries() -> Iterator[Tuple[str, List[str]]]:
				def get_line_from_row(row: PageElement) -> str:
					return row[1].replace("\n", "").strip()

				table_settings = {
					"vertical_strategy": "lines",
					"horizontal_strategy": "lines",
				}

				for page in pdf.pages:
					tables = page.extract_tables(table_settings)
					for table in tables:
						table_iter = peekable(islice(table, 1, None))
						for row in table_iter:
							group = row[0].strip()
							lines = [get_line_from_row(row)]
							while table_iter and not table_iter.peek()[0]:
								row = next(table_iter, None)
								lines.append(get_line_from_row(row))
							yield group, lines

			journal = Journal()
			for group, lines in get_entries():
				for line in lines:
					if note_match := self.note_text_regex.fullmatch(line):
						note = note_match.group(1)
					elif cassi_match := self.cassi_text_regex.fullmatch(line):
						journal.iso4 = cassi_match.group(1)
						note = cassi_match.group(2)
					elif url_match := self.url_regex.fullmatch(line):
						url = line
					else:
						if journal is not None and journal.names and journal.iso4:
							yield journal
							journal = Journal()

						journal.names.update(name.strip() for name in line.split(";"))

			if journal is not None and journal.names and journal.iso4:
				yield journal

			return
			yield

		return
		yield


class UbcFetcher(Fetcher):
	"""
	Source: University of British Columbia, Library
	"""

	name = "UBC"
	url = r"https://journal-abbreviations.library.ubc.ca/dump.php"

	def __init__(self):
		super().__init__()

	def fetch(self) -> Iterator[Journal]:
		resp = requests.get(self.url)
		resp.raise_for_status()

		json_obj = json.loads(resp.text.strip("();"))
		html = BeautifulSoup(json_obj["html"], "html.parser")

		table = html.find("table")
		rows = table.find_all("tr")
		rows_iter = iter(rows)

		while not self._is_canceling:
			row = next(rows_iter, None)
			if row is None:
				break

			cells = row.find_all("td")

			if len(cells) == 2:
				journal = Journal()
				journal.names.add(cells[1].text.strip())
				journal.iso4 = cells[0].text.strip()

				yield journal

		return
		yield


class MdpiFetcher(Fetcher):
	"""
	Source: Molecular Diversity Preservation International
	"""

	name = "MDPI"
	url = r"https://www.mdpi.org/molecules/journallist.htm"

	def __init__(self):
		super().__init__()

	def fetch(self) -> Iterator[Journal]:
		resp = requests.get(self.url)
		resp.raise_for_status()

		html = BeautifulSoup(resp.text, "html.parser")

		paras = html.find_all("p")
		paras_iter = iter(paras)

		# Skip first paragraph (containing last-updated date).
		next(paras_iter)

		while not self._is_canceling:
			para = next(paras_iter, None)
			if para is None:
				break

			para_text = para.text.strip()
			if not para_text:
				continue
			parts = tuple(para_text.split("\\"))
			name = parts[0].strip()
			abbrev = parts[1].strip()

			journal = Journal()
			journal.names.add(name)
			journal.iso4 = abbrev

			if name:
				yield journal

		return
		yield


class MathSciNetFetcher(Fetcher):
	"""
	Source: MathSciNet (American Mathematical Society)
	"""

	name = "MathSciNet"
	url = r"https://mathscinet.ams.org/msnhtml/serials.pdf"

	entry_regex = re.compile(normalize_regex(r"""
		(?P<symbols>[*∗§†]*)
		(?P<abbrev>.+?)
		(?:
			[\t\n]
			(?P<info>.+?)
			(?:
				[\t\n]
				\[(?P<alt_info>.+?)\]
				(?:
					[\t\n]
					(?P<extra_info>.+?)
				)??
			)??
		)??
		(?:
			[\t\n]
			ISSN\s+(?P<issn>\d{4}\s?-\s?\d{3}[\dX])\.
		)?
		\n?
	"""), RegexFlag.DOTALL)

	@staticmethod
	def _translate_symbol(symbol: str) -> str:
		if symbol == "∗":
			return "*"
		return symbol

	@staticmethod
	def _extract_info(info: Optional[str], alt_info: Optional[str], extra_info: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
		if info is None:
			return None, None, None
		if alt_info is not None:
			info = info.strip(".")
			alt_info = alt_info.strip(".")
			return info, alt_info, extra_info
		# TODO: Finish implementation.
		return None, None, None

	def __init__(self):
		super().__init__()

	def fetch(self) -> Iterator[Journal]:
		resp = requests.get(self.url)
		resp.raise_for_status()

		content_length = resp.headers.get("Content-Length", None)
		content_length = int(content_length) if content_length is not None else None
		cached_file = cache_in_memory(resp, size = content_length)
		with pdfplumber.open(cached_file) as pdf:
			def get_entries() -> Iterator[str]:
				from pdfplumber.utils import cluster_objects

				small_font_size_threshold = Decimal("8.0")

				def is_font_bold(char: PDFChar) -> bool:
					tag, fontname = char["fontname"].split("+")
					return "BX" in fontname

				def is_font_small(char: PDFChar) -> bool:
					return char["size"] < small_font_size_threshold

				def process_pdf_char(char: PDFChar):
					if is_font_small(char):
						if char["text"] == "o":
							char["text"] = "°"

				x_tolerance = Decimal("3.0")
				y_tolerance = Decimal("3.0")
				min_tab_width = Decimal("6.0")

				for page in pdf.pages:
					device = PDFPageAggregator(
						pdf.rsrcmgr,
						pageno = page.page_number,
						laparams = pdf.laparams,
					)
					interpreter = PDFPageInterpreter(pdf.rsrcmgr, device)
					interpreter.process_page(page.page_obj)

					contents = page.crop(
						(
							Decimal(100),
							Decimal(70 + (200 if page.page_number == 1 else 0)),
							page.width - Decimal(100),
							page.height - Decimal(70),
						),
						relative = False,
					)
					left_column = contents.crop(
						(
							Decimal(0),
							Decimal(0),
							contents.width * Decimal(0.5),
							contents.height,
						),
						relative = True,
					)
					right_column = contents.crop(
						(
							contents.width * Decimal(0.5),
							Decimal(0),
							contents.width,
							contents.height,
						),
						relative = True,
					)

					for column in (left_column, right_column):
						bold_chars = filter(is_font_bold, column.chars)
						bold_char_lines = cluster_objects(bold_chars, "top", y_tolerance)
						bold_line_y0s = (min(char["top"] for char in line) for line in bold_char_lines)

						hsep_y0s = chain(bold_line_y0s, (column.bbox[3],))
						hsep_y0s = list(hsep_y0s)
						for y0, y1 in windowed(hsep_y0s, 2):
							if y1 is None:
								break
							entry = column.within_bbox(
								(
									column.bbox[0],
									max(y0 - y_tolerance, column.bbox[1]),
									column.bbox[2],
									min(y1 + y_tolerance, column.bbox[3]),
								),
								relative = False,
							)

							yield extract_text2(entry.chars, interpreter, process_pdf_char, min_tab_width, x_tolerance, y_tolerance)

				return
				yield

			def normalize_field(s: str) -> str:
				import unicodedata

				if s is None:
					return None

				io = StringIO()
				last_line = None
				for line in s.splitlines():
					if last_line is not None:
						if len(last_line) == 0:
							pass
						elif len(last_line) > 1 and unicodedata.category(last_line[-1]) in ("Pc", "Pd"):
							pass
						else:
							io.write(" ")

					io.write(line)
					last_line = line

				return io.getvalue()

			journal = Journal()
			for entry in get_entries():
				entry_match = self.entry_regex.fullmatch(entry)
				symbols = set(self._translate_symbol(symbol) for symbol in entry_match.group("symbols"))
				abbrev = normalize_field(entry_match.group("abbrev"))
				info = normalize_field(entry_match.group("info"))
				alt_info = normalize_field(entry_match.group("alt_info"))
				extra_info = normalize_field(entry_match.group("extra_info"))
				issn = normalize_field(entry_match.group("issn"))

				# TODO: Finish implementation.
				# print(f"ENTRY: {symbols} / {abbrev} / {info} / {alt_info} / {extra_info} / {issn}")
				name, alt_name, publisher = self._extract_info(info, alt_info, extra_info)
				print(f"ENTRY: {name} / {alt_name} / {publisher}")

			if journal is not None and journal.names and journal.iso4:
				yield journal

		return
		yield
