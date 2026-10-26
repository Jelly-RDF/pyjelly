## What is Jelly and pyjelly?

**[Jelly]({{ proto_link() }})** is a high-performance serialization format for RDF knowledge graphs and knowledge graph streams. It's designed to be fast, compact, and flexible. 

With Jelly, you can transmit both flat and structured streams of triples, quads, graphs, and datasets. Jelly works well in both batch and real-time settings – including files, sockets, or streaming protocols like Kafka or gRPC.

**pyjelly** is a Python implementation of the Jelly protocol. It provides:

* Full support for reading and writing Jelly-encoded RDF data
* Reading and writing SPARQL query results in [Jelly-SPARQL](sparql.md)
* Seamless integration with [RDFLib](https://rdflib.readthedocs.io/) (*"works just like Turtle"*)
* Standalone generic API with no third-party dependencies [Generic API](generic-sink.md)
* Support for all Jelly stream types
* Tools for working with delimited and non-delimited Jelly streams
* Fine-grained control over serialization options, compression, and framing

## Overview

### Jelly-RDF versions

pyjelly reads all versions of Jelly-RDF: 1.0, 1.1 and 1.2. It writes [Jelly-RDF 1.2]({{ proto_link("specification/serialization/") }}) by default, which is smaller and faster than Jelly-RDF 1.1.

Older Jelly parsers can't read Jelly-RDF 1.2. To write Jelly-RDF 1.1 for them, set the protocol version to 2:

```python
from pyjelly.options import StreamParameters
from pyjelly.serialize.streams import SerializerOptions

options = SerializerOptions(params=StreamParameters(version=2))
graph.serialize("out.jelly", format="jelly", options=options)
```

### Supported stream types

pyjelly writes the `TRIPLES` and `QUADS` [physical stream types]({{ proto_link("specification/serialization/#physical-stream-types") }}). It can also read `GRAPHS`, which only exists in Jelly-RDF 1.0 and 1.1.

A Jelly-RDF 1.2 stream is either one graph or dataset, or a sequence of [RDF Messages]({{ proto_link("specification/serialization/#stream-types") }}). When you write a stream of graphs or datasets (e.g., with `grouped_stream_to_file`), each is an RDF Message. When you read such a stream with `parse_jelly_grouped`, you get one graph or dataset per message.

### RDF 1.2

The generic API supports [RDF 1.2](https://www.w3.org/TR/rdf12-concepts/): triple terms, as a `Triple` in the object of a statement, and directional language-tagged strings, as `Literal("text", "en", direction="ltr")`. To declare the RDF version for a stream, set `rdf_version` in `StreamParameters`. RDFLib 7 doesn't support RDF 1.2 yet.

### SPARQL query results

pyjelly reads and writes the results of `SELECT` and `ASK` queries in [Jelly-SPARQL]({{ proto_link("specification/sparql/") }}), with RDFLib's `Result.parse` and `Result.serialize`, or with the generic API. See [SPARQL query results](sparql.md).

### Conformance to the Jelly specification

pyjelly is continuously tested for conformance to [the Jelly specification]({{ proto_link("specification/") }}). It passes the [conformance tests of Jelly-RDF 1.2]({{ proto_link("conformance/rdf-test-cases/") }}), and the [tests of Jelly-RDF 1.1]({{ proto_link("conformance/rdf-1-1-test-cases/") }}), except for writing the `GRAPHS` physical stream type. It also passes the [conformance tests of Jelly-SPARQL]({{ proto_link("conformance/sparql-test-cases/") }}).

See the [conformance tests]({{ git_link("tests/conformance_tests") }}) in the pyjelly repository.

## Use cases

Use cases for pyjelly include:

- **Client-server communication** – link your client app in Python to the server (e.g., [Apache Jena, RDF4J](https://w3id.org/jelly/jelly-jvm)) with Jelly to reduce latency and improve user experience.
- **Inter-service communication** – use Jelly to efficiently exchange RDF data between microservices.
- **Data science workflows** – use Jelly to read and write RDF data in data science pipelines, enabling efficient processing of large datasets. 
    - pyjelly is fully streaming, so it can handle large datasets without loading everything into memory at once.
    - We are working on support for pandas and other data science libraries – stay tuned for updates!
- **Database dumps and bulk loads** – quickly read and write large RDF datasets with Jelly, reducing storage space and improving database maintenance tasks.

## pyjelly integration with external libraries

To learn how to use popular third-party libraries that connect with RDFLib, please see:

- **[RDFLib-Neo4j integration](rdflib-neo4j-integration.md)**
- **[NetworkX integration](networkx-integration.md)**

## Generic API

pyjelly includes its own [generic API](generic-sink.md) for working with RDF data (no RDFLib or other external RDF libraries required).  
It provides simple, built-in types for triples and quads, allowing you to create, read, and write data directly in the Jelly format.

## Pre-compiled binaries

pyjelly is written in pure Python, so you can run it on any platform that supports Python. We also provide pre-compiled binaries using [mypyc](https://mypyc.readthedocs.io/en/latest/) for better performance. Supported platforms include Windows, macOS, and Linux (x86_64 and ARM64). When installing `pyjelly`, your package manager will automatically download the right binary, or, if your platform is not supported, it will automatically fall back to the pure Python implementation. You can find the full list of supported platforms on [pypi](https://pypi.org/project/pyjelly/#files).

In rudimentary benchmarks, the mypyc-compiled version of pyjelly is 20–80% faster than the pure Python version, depending on the workload.
