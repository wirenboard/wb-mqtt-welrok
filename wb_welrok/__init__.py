from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("wb_welrok")
except Exception:
    __version__ = "0.0.0"
