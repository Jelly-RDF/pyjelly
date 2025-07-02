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

### Serialization

To serialize a graph to the Jelly format:

{{ code_example('rdflib/01_serialize.py') }}

This creates a [delimited Jelly stream]({{ proto_link("user-guide/#delimited-vs-non-delimited-jelly") }}) using default options.

### Parsing

To load RDF data from a `.jelly` file:

{{ code_example('rdflib/02_parse.py') }}

RDFLib will reconstruct the graph from the serialized Jelly stream.

### Streaming graph parser

To process a Jelly stream frame-by-frame, loading each as a separate RDFLib graph:

{{ code_example('rdflib/04_parse_grouped.py') }}

Because `parse_jelly_grouped` returns a generator, each iteration receives **one** graph, keeping memory usage bounded to the current frame. Thus, large datasets and live streams can be processed efficiently.

### Streaming event parser

To process a Jelly stream as a stream of RDFLib triples:

{{ code_example('rdflib/05_parse_flat.py') }}

Here, `parse_jelly_flat` returns a generator of stream events (i.e., statements parsed), allowing for efficient triple-level processing and building custom aggregations from the stream.

### File extension support

You can generally omit the `format="jelly"` parameter if the file ends in `.jelly` – RDFLib will auto-detect the format:

{{ code_example('rdflib/03_parse_autodetect.py') }}

!!! warning 

    Unfortunately, the way this is implemented in RDFLib is a bit wonky, so it will only work if you explicitly import `pyjelly.integrations.rdflib`, or you used `format="jelly"` in the `serialize()` or `parse()` call before.
