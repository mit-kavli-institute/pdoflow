from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = "0.1.14-beta.2"
except PackageNotFoundError:
    __version__ = "0.1.14-beta.2"

__all__ = ["__version__"]
