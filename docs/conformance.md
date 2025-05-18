## Interop & Compatibility

### Supported stream types

`pyjelly` supports all [*physical* stream types]({{ proto_link("specification/reference/#physicalstreamtype") }}) including `TRIPLES`, `QUADS` and `GRAPHS`.

However, only the following [*logical* stream types]({{ proto_link("specification/serialization/#logical-stream-types") }}) are currently supported: `UNSPECIFIED`, `FLAT_TRIPLES`, `FLAT_QUADS`.

> ❌ Grouped logical stream types are **not yet supported**. Frames with grouped logical types will raise an error at parse time.

See the full [stream type matrix]({{ proto_link("specification/serialization/#version-compatibility-and-base-types") }}) for an overview of valid combinations.

### Supported use cases

`pyjelly` is suitable for:

* Compact serialization of large RDF graphs and datasets
* Incremental or streaming processing of RDF data
* Writing or reading `.jelly` files in data pipelines
* Efficient on-disk storage of RDF collections
* Interchange of RDF data between systems

### Conformance to the Jelly protocol

pyjelly is designed to conform to version `{{ proto_version() }}` of the [Jelly specification]({{ proto_link("specification/") }}). It adheres to:

* Stream header structure and metadata
* Frame structure and ordering guarantees
* Compression rules and lookup tables
* Namespace declarations and stream options

Parsing includes automatic validation of conformance, with specific exceptions (`JellyConformanceError`, etc.) raised when violations occur.

### Limitations

* Grouped logical stream types are not yet supported
* Quoted graphs (RDF-star nested triples) are not supported
* Multi-dataset streams cannot currently be parsed into a single `Dataset`
* Logical stream type detection is not automatic; it must be set explicitly via options
