from importlib.metadata import version

try:
    __version__ = version("cubed-xarray")
except Exception:
    __version__ = "999"
