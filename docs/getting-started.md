# Getting started

This guide shows how to install pyjelly and prepare your environment for use with RDFLib.

## Installation

Install pyjelly from PyPI:

```
pip install pyjelly
```

pyjelly requires **Python 3.9** or newer and works on all major platforms (Linux, macOS, Windows).


## Usage with RDFLib

Once installed, pyjelly integrates with RDFLib automatically. You can immediately serialize and parse `.jelly` files using the standard RDFLib API.

### Serialization

To serialize a graph to the Jelly format:

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

g.serialize(destination="foaf.jelly", format="jelly", stream=stream)
```

This creates a [delimited Jelly stream]({{ proto_link("user-guide/#delimited-vs-non-delimited-jelly") }}) using default options.

### Parsing

To load RDF data from a `.jelly` file:

```python
from rdflib import Graph

g = Graph()
g.parse("foaf.jelly")

print("Parsed triples:")
for s, p, o in g:
    print(f"{s} {p} {o}")
```

RDFLib will reconstruct the graph from the serialized Jelly stream.

### File extension support

You can omit the `format="jelly"` parameter if the file ends in `.jelly` – RDFLib will auto-detect the format using pyjelly's entry point:

```python
g.parse("foaf.jelly")  # format inferred automatically
```
