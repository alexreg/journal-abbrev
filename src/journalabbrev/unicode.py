from typing import *
import unicodedata

def _strip_prefix(s: str, prefix: str) -> str:
	return s[len(prefix):] if s.startswith(prefix) else s

def make_combining_form(diacritic: str) -> Optional[str]:
	if unicodedata.category(diacritic) not in ("Sk", "Lm"):
		return None

	name = unicodedata.name(diacritic)
	name = _strip_prefix(name, "MODIFIER LETTER ")
	name = _strip_prefix(name, "COMBINING ")
	try:
		return unicodedata.lookup("COMBINING " + name)
	except KeyError:
		return None
