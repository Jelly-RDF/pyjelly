## What is Jelly and pyjelly?

**[Jelly]({{ proto_link() }})** is a high-performance serialization format for RDF knowledge graphs and knowledge graph streams. It's designed to be fast, compact, and flexible. 

With Jelly, you can transmit both flat and structured streams of triples, quads, graphs, and datasets. Jelly works well in both batch and real-time settings – including files, sockets, or streaming protocols like Kafka or gRPC.

**pyjelly** is a Python implementation of the Jelly protocol. It provides:

* Full support for reading and writing Jelly-encoded RDF data
* Seamless integration with [RDFLib](https://rdflib.readthedocs.io/) (*"works just like Turtle"*)
* Support for all Jelly stream types
* Tools for working with delimited and non-delimited Jelly streams
* Fine-grained control over serialization options, compression, and framing

## Overview

### Supported stream types

pyjelly supports all [physical stream types]({{ proto_link("specification/reference/#physicalstreamtype") }}): `TRIPLES`, `QUADS` and `GRAPHS`.

See the full [stream type matrix]({{ proto_link("serialization/#consistency-with-physical-stream-types") }}) for an overview of valid combinations.

### Conformance to the Jelly specification

pyjelly is continuously tested for conformance to [the Jelly specification]({{ proto_link("specification/") }}). While the vast majority of features are implemented, there are a few edge cases left to resolve.

You can track the progress in [the conformance test suite definition]({{ git_link("tests/utils/rdf_test_cases.py") }}).

## Use cases

Use cases for pyjelly include:

- **Client-server communication** – link your client app in Python to the server (e.g., [Apache Jena, RDF4J](https://w3id.org/jelly/jelly-jvm)) with Jelly to reduce latency and improve user experience.
- **Inter-service communication** – use Jelly to efficiently exchange RDF data between microservices.
- **Data science workflows** – use Jelly to read and write RDF data in data science pipelines, enabling efficient processing of large datasets. 
    - pyjelly is fully streaming, so it can handle large datasets without loading everything into memory at once.
    - We are working on support for pandas and other data science libraries – stay tuned for updates!
- **Database dumps and bulk loads** – quickly read and write large RDF datasets with Jelly, reducing storage space and improving database maintenance tasks.
