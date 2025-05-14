class JellyConformanceError(Exception):
    """Raised when Jelly conformance is violated."""


class JellyAssertionError(AssertionError):
    """Raised when a recommended assertion from the specification fails."""


class JellyNotImplementedError(NotImplementedError):
    """Raised when a future feature is not yet implemented."""
