from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("wb-mqtt-welrok")
except PackageNotFoundError:
    __version__ = "unknown"
