import io
import typing

import pytest

from pyjelly.parse.ioutils import get_options_and_frames


# Regression test for https://github.com/Jelly-RDF/pyjelly/issues/298
def test_get_options_and_frames_no_seek(monkeypatch: pytest.MonkeyPatch) -> None:
    # Sample data: delimited Jelly file with options
    data = b"\x11\x0a\x0f\x0a\x0d\x10\x01\x48\x80\x01\x50\x0f\x58\x0f\x70\x01\x78\x01"
    # Make a bytes IO that does not support seek
    inp = io.BytesIO(data)
    monkeypatch.setattr(inp, "seekable", lambda: False)

    def seek(
        offset: int,  # noqa: ARG001
        whence: typing.Any,  # noqa: ARG001
    ) -> None:
        raise io.UnsupportedOperation

    monkeypatch.setattr(inp, "seek", seek)
    get_options_and_frames(inp)
