import pytest

from pyjelly.errors import JellyConformanceError
from pyjelly.producing.encoder import TermEncoder


def test_encode_literal_fails_with_disabled_datatype_lookup() -> None:
    encoder = TermEncoder(
        name_lookup_size=8,
        prefix_lookup_size=8,
        datatype_lookup_size=0,  # lookup disabled
    )
    with pytest.raises(
        JellyConformanceError,
        match="datatype lookup cannot be used if disabled",
    ):
        encoder.encode_literal(
            lex="42",
            datatype="http://www.w3.org/2001/XMLSchema#integer",
        )
