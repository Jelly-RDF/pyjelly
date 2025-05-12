import rdflib
import rdflib.util

from pyjelly import options


def register_extension_to_rdflib(extension: str = ".jelly") -> None:
    """
    Make [rdflib.util.guess_format][] discover Jelly format.

    >>> rdflib.util.guess_format("foo.jelly")
    >>> register_extension_to_rdflib()
    >>> rdflib.util.guess_format("foo.jelly")
    'jelly'
    """
    rdflib.util.SUFFIX_FORMAT_MAP[extension.removeprefix(".")] = "jelly"


def _side_effects() -> None:
    register_extension_to_rdflib()


if options.INTEGRATION_SIDE_EFFECTS:
    _side_effects()
