from collections.abc import Generator

from pyjelly import producers, stax
from pyjelly.pb2 import rdf_pb2 as pb


def begin(logic: stax.StreamLogic) -> pb.RdfStreamFrame | None:
    return logic.add_row(pb.RdfStreamRow(options=producers.produce_options(logic)))
