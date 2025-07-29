# Getting started

This guide walks you through installing and working with pyjelly.

## Installation (with RDFLib)

Install pyjelly from PyPI:

```bash
pip install pyjelly[rdflib]
```

### Requirements

- Python 3.9 or newer  
- Linux, macOS, or Windows

## Usage with RDFLib

Once you install pyjelly, it integrates automatically with RDFLib through standard RDFLib API.

### Serializing a graph

To serialize a graph to the Jelly format see:

{{ code_example('rdflib/01_serialize.py') }}

This creates a [delimited Jelly stream]({{ proto_link("user-guide/#delimited-vs-non-delimited-jelly") }}) using default options.

### Parsing a graph

To load RDF data from a `.jelly` file see:

{{ code_example('rdflib/02_parse.py') }}

RDFLib will reconstruct the graph from the Jelly file.

### Parsing a stream of graphs

You can process a Jelly stream as a stream of graphs.

In this example, we use a [dataset of weather measurements](https://w3id.org/riverbench/datasets/lod-katrina/dev). We count the number of triples in each graph:

{{ code_example('rdflib/04_parse_grouped.py') }}

Each iteration receives **one** graph, allowing for processing large datasets efficiently.

### Parsing a stream of triples

You can also process a Jelly stream as a flat stream of triples.

We look through a fragment of Denmark's OpenStreetMap to find all city names:

{{ code_example('rdflib/05_parse_flat.py') }}

This case allows you to efficiently process the file triple-by-triple and build custom aggregations from the stream.

### Serializing a stream of graphs

If you have a generator object containing graphs, you can easily serialize it into the Jelly format: 

{{ code_example('rdflib/06_serialize_grouped.py')}}

Allowing for transmitting logically grouped data.  
For more precise control over frame serialization you can use [lower-level API](api.md)

### Serializing a stream of statements

If you have a generator object containing statements, you can easily serialize it into the Jelly format: 

{{ code_example('rdflib/07_serialize_flat.py')}}

The flat method transmits the data as a continuous sequence of statements, keeping it simple.
For more precise control over frame serialization you can use [lower-level API](api.md)

### File extension support

RDFLib will auto-detect the format, so its not always necessary to pass `format="jelly"` parameter:

{{ code_example('rdflib/03_parse_autodetect.py') }}

!!! warning 

    Unfortunately, the way this is implemented in RDFLib is a bit wonky, so it will only work if you explicitly import `pyjelly.integrations.rdflib`, or you used `format="jelly"` in the `serialize()` or `parse()` call before.