## What is Jelly and what is `pyjelly`?

[Jelly]({{ proto_link() }}) is a serialization format and streaming protocol for RDF knowledge graphs. It enables fast, compact, and flexible transmission of RDF data with Protobuf, supporting both flat and structured streams of triples, quads, graphs, and datasets. Jelly is designed to work well in both batch and real-time settings, including use over files, sockets, or stream processing systems like Kafka or gRPC.

**`pyjelly`** is a Python implementation of the Jelly protocol. It provides:

* Full support for reading and writing Jelly-encoded RDF data
* Seamless integration with [RDFLib](https://rdflib.readthedocs.io/) (*"works just like Turtle"*)
* Support for all Jelly stream types
* Tools for working with delimited and non-delimited Jelly streams
* Fine-grained control over serialization options, compression, and framing
