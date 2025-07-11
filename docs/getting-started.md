# Getting started

This guide shows how to install pyjelly and prepare your environment for use with RDFLib.

## Installation (with RDFLib)

Install pyjelly from PyPI:

```
pip install pyjelly[rdflib]
```

pyjelly requires **Python 3.9** or newer and works on all major platforms (Linux, macOS, Windows).


## Usage with RDFLib

Once installed, pyjelly integrates with RDFLib automatically. You can immediately serialize and parse `.jelly` files using the standard RDFLib API.

### Serializing a graph

To serialize a graph to the Jelly format:

{{ code_example('rdflib/01_serialize.py') }}

This creates a [delimited Jelly stream]({{ proto_link("user-guide/#delimited-vs-non-delimited-jelly") }}) using default options.

### Parsing a graph

To load RDF data from a `.jelly` file:

{{ code_example('rdflib/02_parse.py') }}

RDFLib will reconstruct the graph from the Jelly file.

### Parsing a stream of graphs

You can process a Jelly stream as a stream of graphs. A Jelly file consists of "frames" (batches of statements) – we can load each frame as a separate RDFLib graph.

In this example, we use a [dataset of weather measurements](https://w3id.org/riverbench/datasets/lod-katrina/dev), which is an RDF graph stream. We count the number of triples in each graph:

{{ code_example('rdflib/04_parse_grouped.py') }}

Because `parse_jelly_grouped` returns a generator, each iteration receives **one** graph, keeping memory usage bounded to the current frame. So, large datasets and live streams can be processed efficiently.

### Parsing a stream of triples

You can also process a Jelly stream as a flat stream of triples.

In this more complex example, we look through a fragment of Denmark's OpenStreetMap to find all city names:

{{ code_example('rdflib/05_parse_flat.py') }}

`parse_jelly_flat` returns a generator of stream events (i.e., statements parsed). This allows you to efficiently process the file triple-by-triple and build custom aggregations from the stream.

### Serializing a stream of graphs

If you have a generator object containing graphs, you can easily serialize it into the Jelly format, like in the following example: 

{{ code_example('rdflib/06_serialize_grouped.py')}}

This method allows for transmitting logically grouped data, preserving their original division.
For more precise control over frame serialization you can use [lower-level API](api.md)

### Serializing a stream of statements

If you have a generator object containing statements, you can easily serialize it into the Jelly format, like in the following example: 

{{ code_example('rdflib/07_serialize_flat.py')}}

The flat method transmits the data as a continuous sequence of individual statements (i.e., triples or quads), keeping the simplicity and order of the data.
For more precise control over frame serialization you can use [lower-level API](api.md)

### Serializing a stream of graphs

If you have a generator object containing graphs, you can easily serialize it into the Jelly format, like in the following example: 

{{ code_example('rdflib/06_serialize_grouped.py')}}

This method allows for transmitting logically grouped data, preserving their original division.
For more precise control over frame serialization you can use [lower-level API](api.md)

### File extension support

You can generally omit the `format="jelly"` parameter if the file ends in `.jelly` – RDFLib will auto-detect the format:

{{ code_example('rdflib/03_parse_autodetect.py') }}

!!! warning 

    Unfortunately, the way this is implemented in RDFLib is a bit wonky, so it will only work if you explicitly import `pyjelly.integrations.rdflib`, or you used `format="jelly"` in the `serialize()` or `parse()` call before.