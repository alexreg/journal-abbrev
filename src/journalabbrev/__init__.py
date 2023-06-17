from importlib.metadata import distribution
from typing import *

pkg_metadata = dict(distribution("journal-abbrev").metadata.json.items())

__all__ = ["__version__", "__author__"]
__version__ = pkg_metadata["Version"]
__author__ = pkg_metadata["Author"]
