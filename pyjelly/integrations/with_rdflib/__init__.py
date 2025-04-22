import rdflib.util


def register_extension_to_rdflib(extension: str = ".jelly") -> None:
    """
    Make [rdflib.util.guess_format][] discover Jelly format.

    >>> rdflib.util.guess_format("foo.jelly")
    >>> register_extension_to_rdflib()
    >>> rdflib.util.guess_format("foo.jelly")
    'jelly'
    """
    rdflib.util.SUFFIX_FORMAT_MAP[extension.removeprefix(".")] = "jelly"
