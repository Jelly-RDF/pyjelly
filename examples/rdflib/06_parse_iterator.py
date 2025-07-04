import tarfile
import urllib.request
from pyjelly import jelly
from pyjelly.options import StreamParameters
from rdflib import Graph
from pyjelly.serialize.streams import SerializerOptions, TripleStream
from pyjelly.serialize.ioutils import write_delimited

URL = "https://w3id.org/riverbench/datasets/lod-katrina/dev/files/stream_10K.tar.gz"
OUTPUT = "streamed_output.jelly"

# prepare a graph stream object
stream = TripleStream.for_rdflib(
    options=SerializerOptions(
        logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
        params=StreamParameters(delimited=False),
    )
)
stream.enroll()  # emit the options frame

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
    # parse the graph files into the output file, frame per graph
    for graph in graphs:
        for triple in graph:
            stream.triple(triple)

        # one frame for graph
        if frame := stream.flow.to_stream_frame():
            write_delimited(frame, out)

    # flush the last remaining frame to the output file
    if flush_frame := stream.flow.to_stream_frame():
        write_delimited(flush_frame, out)

print("Done.")
