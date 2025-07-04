import tarfile
import urllib.request
from pyjelly import jelly
from pyjelly.options import StreamParameters
from rdflib import Graph
from pyjelly.integrations.rdflib.serialize import triples_stream_frames
from pyjelly.serialize.streams import SerializerOptions, TripleStream
from pyjelly.serialize.ioutils import write_delimited

URL = "https://w3id.org/riverbench/datasets/lod-katrina/dev/files/stream_10K.tar.gz"
OUTPUT = "streamed_output.jelly"

# prepare a graph stream object
stream = TripleStream.for_rdflib(
    options=SerializerOptions(
        logical_type=jelly.LOGICAL_STREAM_TYPE_GRAPHS,
        params=StreamParameters(),
    )
)

print(f"Writing Jelly frames to {OUTPUT!r}…")
with (
    urllib.request.urlopen(URL) as resp,
    tarfile.open(fileobj=resp, mode="r:gz") as tar,
    open(OUTPUT, "wb") as out,
):
    # build graphs from a .ttl stream file
    graphs = (
        (g := Graph(), g.parse(source=f, format="turtle"), g)[2]
        for member in tar
        if member.name.endswith(".ttl") and (f := tar.extractfile(member)) is not None
    )
    # serialize the graph files into the output file, frame per graph
    # fmt: off
    for graph in graphs:
        if frames := next(triples_stream_frames(stream, graph)): # type: ignore[arg-type]
            write_delimited(frames, out)
    # fmt: on

print("Done.")
