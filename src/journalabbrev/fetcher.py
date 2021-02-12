from abc import *
from bs4 import BeautifulSoup
from bs4.element import PageElement, ResultSet
import json
from more_itertools import peekable
import pdfplumber
import requests

from .common import *
from .db import *


class Fetcher:
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
			raise ProcessingError(f"unexpected first row of table")

		while not self._is_canceling:
			cur_row = next(rows_iter, None)
			if cur_row is None:
				break

			cells = cur_row.find_all("td")
			lines = list(l.replace("\n", " ") for l in (l.strip() for l in cells[0].text.split("\n", maxsplit = 1)) if len(l))

			journal = Journal()
			journal.coden = cells[1].text.strip()
			if len(lines) == 2:
				journal.iso4, journal.name = lines
			elif len(lines) == 1:
				journal.iso4 = lines[0]
				journal.name = None
				while len(rows_iter.peek().find_all("td")) == 1:
					cur_row = next(rows_iter)
					journal.name = cur_row.find("td").text
				if journal.name is None:
					journal.name = journal.iso4
			else:
				raise ProcessingError(f"unexpected number of lines in cell ({len(lines)})")

			yield journal

		return
		yield


class BeyondCassiFetcher(Fetcher):
	"""
	Source: Beyond CASSI
	"""

	url = r"https://www.cas.org/sites/default/files/documents/beyond_cassi.pdf"

	header_text_regex = re.compile(r"JOURNAL TITLES\b.*")
	note_text_regex = re.compile(r"Note: *(.+?)")
	cassi_text_regex = re.compile(r"CASSI: *(.+?)(?:(?:\.\.\.|…)?\s*\[Note:\s*(.+?)\])?")

	pdf_pos_tolerance = 5

	def __init__(self):
		super().__init__()

	def fetch(self) -> Iterator[Journal]:
		# resp = requests.get(self.url)
		# resp.raise_for_status()

		# content_length = resp.headers.get("Content-Length", None)
		# content_length = int(content_length) if content_length is not None else None
		# cached_file = cache_in_memory(resp, size = content_length)
		# with pdfplumber.open(cached_file) as pdf:
		with pdfplumber.open('/Users/alex/Downloads/beyond_cassi.pdf') as pdf:
			def get_lines():
				table_settings = {
					"vertical_strategy": "lines",
					"horizontal_strategy": "lines",
				}

				for cur_page in pdf.pages:
					tables = cur_page.extract_tables(table_settings)
					for cur_table in tables:
						for cur_row in cur_table:
							text = cur_row[1].replace("\n", "").strip()
							yield text

			for cur_line in get_lines():
				note_match = self.note_text_regex.fullmatch(cur_line)
				cassi_match = self.cassi_text_regex.fullmatch(cur_line)

				if note_match:
					note = note_match.group(1)
					print(f"NOTE: {note}")
				elif cassi_match:
					abbrev = cassi_match.group(1)
					note = cassi_match.group(2)
					print(f"CASSI: {abbrev}")
					if note:
						print(f"CASSI / NOTE: {note}")
				else:
					names = [n.strip() for n in cur_line.split(";")]
					for cur_name in names:
						print(f"NAME: {cur_name}")

		return
		yield


class UbcFetcher(Fetcher):
	"""
	Source: University of British Columbia, Library
	"""

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
			cur_row = next(rows_iter, None)
			if cur_row is None:
				break

			cells = cur_row.find_all("td")

			if len(cells) == 2:
				journal = Journal()
				journal.name = cells[1].text.strip()
				journal.iso4 = cells[0].text.strip()

				yield journal

		return
		yield


class MdpiFetcher(Fetcher):
	"""
	Source: Molecular Diversity Preservation International
	"""

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
			cur_para = next(paras_iter, None)
			if paras_iter is None:
				break

			cur_para_text = cur_para.text.strip()
			if len(cur_para_text) == 0:
				continue
			parts = tuple(cur_para_text.split("\\"))

			journal = Journal()
			journal.name = parts[0].strip()
			journal.iso4 = parts[1].strip()

			if len(journal.name) > 0:
				yield journal
			print(journal.asdict())

		return
		yield
