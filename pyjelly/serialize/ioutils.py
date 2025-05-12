from typing import IO

from google.protobuf.proto import serialize_length_prefixed

from pyjelly import jelly


def write_delimited(frame: jelly.RdfStreamFrame, output_stream: IO[bytes]) -> None:
    serialize_length_prefixed(frame, output_stream)


def write_single(frame: jelly.RdfStreamFrame, output_stream: IO[bytes]) -> None:
    output_stream.write(frame.SerializeToString(deterministic=True))
