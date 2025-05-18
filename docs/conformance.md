## Interop & Compatibility

### Supported stream types

`pyjelly` supports all *physical* stream types defined in the [Jelly protocol specification]({{ proto_link("spec/stream-types") }}), including:

* [`TRIPLES`]({{ proto_link("spec/stream-types#triples") }})
* [`QUADS`]({{ proto_link("spec/stream-types#quads") }})
* [`GRAPHS`]({{ proto_link("spec/stream-types#graphs") }})

However, only the following *logical* stream types are currently supported:

* [`UNSPECIFIED`]({{ proto_link("spec/stream-types#unspecified") }})
* [`FLAT_TRIPLES`]({{ proto_link("spec/stream-types#flat-triple-stream") }})
* [`FLAT_QUADS`]({{ proto_link("spec/stream-types#flat-quad-stream") }})

> ❌ Grouped logical stream types (e.g. [`GRAPH_STREAM`]({{ proto_link("spec/stream-types#graph-stream") }}), [`DATASET_STREAM`]({{ proto_link("spec/stream-types#dataset-stream") }}) etc.) are **not yet supported**. Frames with grouped logical types will raise an error at parse time.

See the full [stream type matrix]({{ proto_link("spec/stream-types#rdf-stax-logical-type--physical-type") }}) for an overview of valid combinations.

### Supported use cases

`pyjelly` is suitable for:

* Compact serialization of large RDF graphs and datasets
* Incremental or streaming processing of RDF data
* Writing or reading `.jelly` files in data pipelines
* Efficient on-disk storage of RDF collections
* Interchange of RDF data between systems

### Conformance to the Jelly protocol

`pyjelly` is designed to conform to version `{{ proto_version() }}` of the [Jelly specification]({{ proto_link("spec/") }}). It adheres to:

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
