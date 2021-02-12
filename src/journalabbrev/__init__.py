from setuptools.config import read_configuration

config = read_configuration("setup.cfg")
metadata = config["metadata"]

__all__ = ["__version__", "__author__"]
__version__ = metadata["version"]
__author__ = metadata["author"]
