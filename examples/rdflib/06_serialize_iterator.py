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

# open resources through tarball
resp = urllib.request.urlopen(URL)
tar = tarfile.open(fileobj=resp, mode="r:gz")
out = open(OUTPUT, "wb")

# iterate through each dataset in the stream
print(f"Writing Jelly frames to {OUTPUT!r}…")
for member in tar:
    if not member.name.endswith(".ttl"):
        continue
    f = tar.extractfile(member)
    if not f:
        continue

    # parse into an rdflib.Graph
    graph = Graph()
    graph.parse(source=f, format="turtle")

    # serialize the graph files into the output file, frame per graph
    if frames := next(triples_stream_frames(stream, graph)):
        write_delimited(frames, out)

print("Done.")

# close files
out.close()
tar.close()
resp.close()
