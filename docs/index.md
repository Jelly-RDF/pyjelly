# pyjelly

[![PyPI – Version](https://img.shields.io/pypi/v/pyjelly)](https://pypi.org/project/pyjelly/) [![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pyjelly)](https://pypi.org/project/pyjelly/) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) [![CI status](https://github.com/Jelly-RDF/pyjelly/actions/workflows/ci.yml/badge.svg)](https://github.com/Jelly-RDF/pyjelly/actions/workflows/ci.yml) [![Code coverage](https://codecov.io/gh/Jelly-RDF/pyjelly/branch/main/graph/badge.svg?token=2D8M2QH6U0)](https://codecov.io/gh/Jelly-RDF/pyjelly) [![Discord](https://img.shields.io/discord/1333391881404420179?label=Discord%20chat)](https://discord.gg/A8sN5XwVa5)

**pyjelly** is a Python implementation of [Jelly]({{ proto_link("") }}), a high-performance binary serialization format and streaming protocol for RDF knowledge graphs. It works great with [rdflib](getting-started.md).

## Features

- **Fast reading and writing** of RDF knowledge graphs in the [Jelly format](http://w3id.org/jelly)
- **Seamless integration with [rdflib](https://rdflib.readthedocs.io/)**
- **Stream processing support** for large datasets or streams of many RDF graphs/datasets

## Documentation

- **[Getting started](getting-started.md)**
- **[Overview and supported features](overview.md)**
- **[Integration with Python RDF Tools](external-integration.md)**
- **[API reference](api.md)**
- **[Contributing](contributing/index.md)** – how to report issues, contribute code, and request features

## Commercial and community support

**[NeverBlink](https://neverblink.eu)** provides commercial support services for Jelly, including implementing custom features, system integrations, implementations for new frameworks, benchmarking, and more.

Community support is available on the **[Jelly Discord chat](https://discord.gg/A8sN5XwVa5)**.

## License

The pyjelly library is licensed under the [Apache 2.0 license](https://github.com/Jelly-RDF/pyjelly/blob/{{ git_tag() }}/LICENSE).

----

The development of the Jelly protocol, its implementations, and supporting tooling was co-funded by the European Union. **[More details]({{ proto_link( 'licensing/projects' ) }})**.

![European Funds for Smart Economy, Republic of Poland, Co-funded by the European Union](assets/featured/feng_rp_eu.png)