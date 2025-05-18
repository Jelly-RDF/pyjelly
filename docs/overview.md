## What is Jelly and what is pyjelly?

[Jelly]({{ proto_link() }}) is a serialization format and streaming protocol for RDF knowledge graphs. It enables fast, compact, and flexible transmission of RDF data with Protobuf, supporting both flat and structured streams of triples, quads, graphs, and datasets. Jelly is designed to work well in both batch and real-time settings, including use over files, sockets, or stream processing systems like Kafka or gRPC.

**pyjelly** is a Python implementation of the Jelly protocol. It provides:

* Full support for reading and writing Jelly-encoded RDF data
* Seamless integration with [RDFLib](https://rdflib.readthedocs.io/) (*"works just like Turtle"*)
* Support for all Jelly stream types
* Tools for working with delimited and non-delimited Jelly streams
* Fine-grained control over serialization options, compression, and framing

## Overview

### Use cases

pyjelly is suitable for:

* Compact serialization of large RDF graphs and datasets.
* Incremental or streaming processing of RDF data.
* Writing or reading `.jelly` files in data pipelines.
* Efficient on-disk storage of RDF collections.
* Interchange of RDF data between systems.

### Supported stream types

pyjelly supports all [*physical* stream types]({{ proto_link("specification/reference/#physicalstreamtype") }}) including `TRIPLES`, `QUADS` and `GRAPHS`.

However, only the following [*logical* stream types]({{ proto_link("specification/serialization/#logical-stream-types") }}) are currently supported: `UNSPECIFIED`, `FLAT_TRIPLES`, `FLAT_QUADS`.

!!! warning
    Grouped logical stream types are **not yet supported**. Frames with grouped logical types will raise an error at parse time.

See the full [stream type matrix]({{ proto_link("serialization/#consistency-with-physical-stream-types") }}) for an overview of valid combinations.

### Conformance to the Jelly protocol

pyjelly is designed to conform to [version {{ proto_version() }} of the Jelly specification]({{ proto_link("specification/") }}). It adheres to:

* Stream header structure and metadata.
* Frame structure and ordering guarantees.
* Compression rules and lookup tables.
* Namespace declarations and stream options.

Parsing includes automatic validation of conformance raised when violations occur.

### Limitations

* Grouped logical stream types are not yet supported.
* Quoted graphs (RDF-star nested triples) are not supported.
* Multi-dataset streams cannot currently be parsed into a single `Dataset`.
* Logical stream type detection is not automatic; it must be set explicitly via options.
