class JellyConformanceError(Exception):
    """Raised when Jelly conformance is violated."""


class JellyAssertionError(AssertionError):
    """Raised when a recommended assertion from the specification fails."""


class JellyNotImplementedError(NotImplementedError):
    """Raised when a future feature is not yet implemented."""


class JellyIncompleteResultsError(Exception):
    """
    Raised when the producer of SPARQL results says that they are incomplete.

    For example, when the query failed midway. The rows read before are valid.
    """

    def __init__(self, message: str, error: str, result: object = None) -> None:
        super().__init__(message)
        #: Why the producer could not complete the results, in its words
        self.error = error
        #: The results that were read, if the reader keeps them
        self.result = result
