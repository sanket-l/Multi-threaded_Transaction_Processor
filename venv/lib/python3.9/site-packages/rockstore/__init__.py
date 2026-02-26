"""
A Pythonic wrapper for RocksDB using CFFI.
"""

from .store import RockStore
from .context import open_database

try:
    from ._version import version as __version__
except ImportError:
    try:
        from importlib.metadata import version

        __version__ = version("rockstore")
    except ImportError:
        __version__ = "0.0.0+unknown"

__all__ = ["RockStore", "open_database"]
