from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = "0.1.15"
except PackageNotFoundError:
    __version__ = "0.1.15"

__all__ = ["__version__"]
