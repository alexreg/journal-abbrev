import re
import unicodedata
from decimal import Decimal
from io import StringIO
from re import RegexFlag
from typing import *

from more_itertools import partition, peekable
from pdfminer.pdffont import PDFCIDFont, PDFFont
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfplumber.utils import DEFAULT_X_TOLERANCE, DEFAULT_Y_TOLERANCE, cluster_objects

from .unicode import *


PDFChar = Dict
ProcessPDFCharFn = Callable[[PDFChar], None]


cid_regex = re.compile(r"\(cid:(\d+)\)", RegexFlag.IGNORECASE)


def cmap_char(
    cid: int, fontname: str, interpreter: PDFPageInterpreter
) -> Optional[str]:
    for font in cast(Iterable[PDFFont], interpreter.fontmap.values()):
        if fontname.casefold() == font.fontname.casefold():
            if isinstance(font, PDFCIDFont):
                font = cast(PDFCIDFont, font)
                return font.to_unichr(cid)
            else:
                return None

    raise ValueError(f"Font '{fontname}' not  found")


def normalize_char(
    char: PDFChar,
    interpreter: PDFPageInterpreter,
    proc_pdf_char: Optional[ProcessPDFCharFn] = None,
) -> Optional[PDFChar]:
    text = char["text"]
    if len(text) > 1 and (cid_match := cid_regex.fullmatch(text)) is not None:
        cid = int(cid_match.group(1))
        text = cmap_char(cid, char["fontname"], interpreter)
        if text is None:
            char["text"] = None
            return char

    ntext = unicodedata.normalize("NFKC", text)
    if len(ntext) == 2 and unicodedata.combining(ntext[1]):
        text = ntext[1]
    char["text"] = make_combining_form(text) or text

    if proc_pdf_char is not None:
        proc_pdf_char(char)

    return char


def sort_line_chars(
    chars: Iterable[PDFChar], interpreter: PDFPageInterpreter
) -> Iterable[PDFChar]:
    chars = sorted(chars, key=lambda char: char["x0"])
    main_chars, combining_chars = partition(
        lambda char: char["text"] and unicodedata.combining(char["text"]), chars
    )
    combining_chars_iter = peekable(iter(combining_chars))
    for main_char in main_chars:
        yield main_char

        while combining_chars_iter:
            combining_char = combining_chars_iter.peek()

            overlap = max(
                min(main_char["x1"], combining_char["x1"])
                - max(main_char["x0"], combining_char["x0"]),
                0,
            )
            if overlap < main_char["width"] * Decimal("0.5"):
                break

            yield combining_char
            next(combining_chars_iter, None)

    assert next(combining_chars_iter, None) is None

    return
    yield


def extract_text2(
    chars: Iterable[PDFChar],
    interpreter: PDFPageInterpreter,
    proc_pdf_char: Optional[ProcessPDFCharFn] = None,
    min_tab_width: Optional[Decimal] = None,
    x_tolerance: Decimal = DEFAULT_X_TOLERANCE,
    y_tolerance: Decimal = DEFAULT_Y_TOLERANCE,
) -> str:
    text = StringIO()

    lines = cluster_objects(chars, "top", y_tolerance)
    for line_chars in lines:
        line_chars = (
            normalize_char(char, interpreter, proc_pdf_char) for char in line_chars
        )
        last_char: Optional[PDFChar] = None
        for char in sort_line_chars(line_chars, interpreter):
            if last_char is not None and last_char["text"] is not None:
                if (
                    min_tab_width is not None
                    and char["x0"] > last_char["x1"] + min_tab_width
                ):
                    text.write("\t")
                elif char["x0"] > last_char["x1"] + x_tolerance:
                    text.write(" ")

            if char["text"] is not None:
                text.write(char["text"])
                if not unicodedata.combining(char["text"]):
                    last_char = char

        text.write("\n")

    return unicodedata.normalize("NFKC", text.getvalue())
