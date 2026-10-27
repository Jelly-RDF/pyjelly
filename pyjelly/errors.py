class JellyConformanceError(Exception):
    """Raised when Jelly conformance is violated."""


class JellyAssertionError(AssertionError):
    """Raised when a recommended assertion from the specification fails."""


class JellyNotImplementedError(NotImplementedError):
    """Raised when a future feature is not yet implemented."""


class JellyIncompleteResultsError(Exception):
    """
    Raised when the producer of SPARQL results reports that they are incomplete.

    For example, the query failed midway. The rows read before the error are valid.
    """

    def __init__(self, message: str, error: str, result: object = None) -> None:
        super().__init__(message)
        #: Error message from the producer
        self.error = error
        #: Results read before the error, if the reader keeps them
        self.result = result


class JellyTruncatedResultsWarning(UserWarning):
    """
    Issued when a stream of SPARQL results ends without a trailer.

    Some results may be missing, e.g., because the writer stopped or the connection
    was lost.
    """
