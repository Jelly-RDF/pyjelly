import gzip
import urllib.request
from typing import cast, IO

from pyjelly.integrations.rdflib.parse import parse_jelly_flat, Triple
from rdflib import Graph, URIRef

predicate_to_look_for = URIRef(
    "http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#floatValue"
)
graph_measurements = Graph()

url = "https://w3id.org/riverbench/datasets/lod-katrina/dev/files/jelly_10K.jelly.gz"
with (
    urllib.request.urlopen(url) as resp,
    cast(IO[bytes], gzip.GzipFile(fileobj=resp)) as jelly_stream,
):
    events = parse_jelly_flat(jelly_stream)
    for event in events:
        if isinstance(event, Triple):  # filter the stream event of interest
            s, p, o = event
            if p == predicate_to_look_for:
                graph_measurements.add(event)
print(f"Measurements in graph: {len(graph_measurements)}")
