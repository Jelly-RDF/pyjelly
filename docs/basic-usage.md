# Basic usage with RDFLib

Once installed, pyjelly integrates automatically with RDFLib via Python entry points. You can immediately serialize and parse `.jelly` files using the standard RDFLib API.

## Serialization

To serialize a `Graph` to the Jelly format:

```python
from rdflib import Graph
from pyjelly.serialize.streams import Stream
from pyjelly.options import StreamOptions, StreamTypes
from pyjelly import jelly

g = Graph()
g.parse("http://xmlns.com/foaf/spec/index.rdf")

options = StreamOptions(
    stream_types=StreamTypes(
        physical_type=jelly.PHYSICAL_STREAM_TYPE_TRIPLES,
        logical_type=jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES,
    )
)
stream = Stream.from_options(options)

with open("foaf.jelly", "wb") as out:
    g.serialize(destination=out, format="jelly", stream=stream)
```

This creates a **delimited** Jelly stream using default options. The output format is determined by the `format="jelly"` parameter or inferred from the `.jelly` file extension.

## Parsing

To load RDF data from a `.jelly` file:

```python
from rdflib import Graph

g = Graph()
g.parse("foaf.jelly", format="jelly")

print("Parsed triples:")
for s, p, o in g:
    print(f"{s} {p} {o}")
```

RDFLib will reconstruct the graph from the serialized Jelly stream.

## File extension support

You can omit the `format="jelly"` parameter if the file ends in `.jelly` – RDFLib will auto-detect the format using pyjelly's entry point:

```python
g.parse("foaf.jelly")  # format inferred automatically
```
