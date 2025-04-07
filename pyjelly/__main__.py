import sys
from collections.abc import Callable
from functools import partial
from pathlib import Path
from typing import IO

import google.protobuf.proto as protolib  # type: ignore[import-not-found]
import rdflib
from rdflib.plugins.stores.memory import SimpleMemory

from pyjelly.pb2 import rdf_pb2 as pb


def _write_callback(file: IO[bytes], frame: pb.RdfStreamFrame) -> None:
    file.write(protolib.serialize(frame))


def writer(file: IO[bytes]) -> Callable[[pb.RdfStreamFrame], None]:
    return partial(_write_callback, file)


def writing_delimited(file: IO[bytes]) -> Callable[[pb.RdfStreamFrame], None]:
    return partial(protolib.serialize_length_prefixed, output=file)  # pyright: ignore


graph = rdflib.Graph(store=SimpleMemory())
graph.parse(location=sys.argv[1])
with Path(sys.argv[2]).open("wb") as fd:
    graph.serialize(encoding="jelly", format="jelly", writer=writer(fd))
