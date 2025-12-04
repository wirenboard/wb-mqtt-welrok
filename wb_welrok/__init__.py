from importlib.metadata import PackageNotFoundError, version

__version__ = "0.0.16"
try:
    __version__ = version("wb-mqtt-welrok")
except PackageNotFoundError:
    __version__ = "unknown"
