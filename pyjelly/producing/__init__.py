from collections.abc import Iterable, Iterator

from pyjelly import jelly
from pyjelly.producing.encoder import Encoder
from pyjelly.producing.producers import FlatProducer


def stream_to_frame(
    *,
    logical_type: jelly.LogicalStreamType = jelly.LOGICAL_STREAM_TYPE_UNSPECIFIED,
    encoder: Encoder,
    statements: Iterable[Iterable[object]],
) -> jelly.RdfStreamFrame:
    rows = [encoder.options_to_stream_row(logical_type=logical_type)]

    for statement in statements:
        encoder.encode_statement(statement)
        rows.extend(encoder.to_stream_rows())

    return jelly.RdfStreamFrame(rows=rows)


def stream_to_frames(
    producer: FlatProducer,
    encoder: Encoder,
    statements: Iterable[Iterable[object]],
) -> Iterator[jelly.RdfStreamFrame]:
    producer.add_stream_rows(
        (encoder.options_to_stream_row(logical_type=producer.jelly_type),)
    )

    for statement in statements:
        encoder.encode_statement(statement)
        if frame := producer.add_stream_rows(encoder.to_stream_rows()):
            yield frame

    if remaining := producer.to_stream_frame():
        yield remaining
