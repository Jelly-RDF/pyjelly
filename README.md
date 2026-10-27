[![Documentation](https://img.shields.io/website?url=https%3A%2F%2Fw3id.org%2Fjelly%2Fpyjelly&label=Documentation)](https://w3id.org/jelly/pyjelly) [![PyPI – Version](https://img.shields.io/pypi/v/pyjelly)](https://pypi.org/project/pyjelly/) [![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pyjelly)](https://pypi.org/project/pyjelly/) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) [![CI status](https://github.com/Jelly-RDF/pyjelly/actions/workflows/ci.yml/badge.svg)](https://github.com/Jelly-RDF/pyjelly/actions/workflows/ci.yml) [![Code coverage](https://codecov.io/gh/Jelly-RDF/pyjelly/branch/main/graph/badge.svg?token=2D8M2QH6U0)](https://codecov.io/gh/Jelly-RDF/pyjelly) [![Discord](https://img.shields.io/discord/1333391881404420179?label=Discord%20chat)](https://discord.gg/A8sN5XwVa5)

# pyjelly

**pyjelly** is a Python implementation of [Jelly](https://w3id.org/jelly), a high-performance binary serialization format and streaming protocol for RDF knowledge graphs.

**Documentation, usage guide and more: https://w3id.org/jelly/pyjelly**

## Features

- **[Fast](https://w3id.org/jelly/dev/performance/rdf/#pyjelly) reading and writing** of RDF knowledge graphs in the [Jelly format](https://w3id.org/jelly). pyjelly writes [Jelly-RDF 1.2](https://w3id.org/jelly/dev/specification/serialization/) by default, and reads Jelly-RDF 1.0–1.2.
- Reading and writing **[SPARQL query results](https://w3id.org/jelly/pyjelly/dev/sparql)** in [Jelly-SPARQL](https://w3id.org/jelly/dev/specification/sparql), with RDFLib or the generic API.
- **Standalone [generic API](https://w3id.org/jelly/pyjelly/dev/generic-sink)** that doesn't need RDFLib or any other RDF library, allowing for:
    - Serialization and parsing of statements to and from Jelly files.
    - Parsing and serializing streams of graphs and statements.
    - [RDF 1.2](https://www.w3.org/TR/rdf12-concepts/) triple terms and literals with a base direction.
- Precise control over **serialization options and framing**.
- **Seamless** integration with: 
    - **[RDFLib](https://w3id.org/jelly/pyjelly/dev/getting-started)**
    - **[RDFLib-Neo4j](https://w3id.org/jelly/pyjelly/dev/rdflib-neo4j-integration)**
    - **[NetworkX](https://w3id.org/jelly/pyjelly/dev/networkx-integration)**
- **Stream processing support** for large datasets and streams of many RDF graphs or datasets.

**pyjelly** is useful when dealing with (see [full description](https://w3id.org/jelly/pyjelly/dev/overview/#use-cases)):

- Dumping and loading **large RDF datasets**.
- **Client-server communication**.
- Workflows, where **streaming** is required.

## Getting started

Install pyjelly from **[PyPI](https://pypi.org/project/pyjelly/)**:

```bash
pip install pyjelly[rdflib]
```

To write an RDF graph to a Jelly file:

```python
from rdflib import Graph

g = Graph()
g.parse("http://xmlns.com/foaf/spec/index.rdf")
g.serialize(destination="foaf.jelly", format="jelly")
```

To read a Jelly file and convert it to an RDFLib `Graph`:

```python
from rdflib import Graph

g = Graph()
g.parse("foaf.jelly", format="jelly")
```

**See [our documentation](https://w3id.org/jelly/pyjelly) for [further examples](https://w3id.org/jelly/pyjelly/dev/getting-started/), a full [API reference](https://w3id.org/jelly/pyjelly/dev/api), and more.**

## Contributing and support

This project is being actively developed – you can stay tuned by [watching this repository](https://docs.github.com/en/account-and-profile/managing-subscriptions-and-notifications-on-github/setting-up-notifications/about-notifications#subscription-options).

Join the **[Jelly Discord chat](https://discord.gg/A8sN5XwVa5)** to ask questions about pyjelly and to be up-to-date with the development activities.

### Commercial support

**[NeverBlink](https://neverblink.eu)** provides commercial support services for Jelly, including implementing custom features, system integrations, implementations for new frameworks, benchmarking, and more.

### Contributing

If you'd like to contribute, check out our [contributing guidelines](CONTRIBUTING.md).

## License

The pyjelly library is licensed under the [Apache 2.0 license](https://www.apache.org/licenses/LICENSE-2.0).

----

The development of the Jelly protocol, its implementations, and supporting tooling was funded by commercial sponsors and the European Union. **[More details](https://w3id.org/jelly/dev/licensing/projects)**.
