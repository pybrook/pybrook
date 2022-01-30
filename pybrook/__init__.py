try:
    import importlib.metadata as importlib_metadata  # noqa: WPS301
except ModuleNotFoundError:  # pragma: no cover
    import importlib_metadata  # type: ignore # noqa: WPS440

VERSION = importlib_metadata.version(__name__)
