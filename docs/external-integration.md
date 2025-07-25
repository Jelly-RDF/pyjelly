# pyjelly integration with external libraries

In this section, you will learn how to use popular third-party libraries that connect with RDFLib.
Before you continue with this document, please review the [getting started chapter](getting-started.md).

## RDFLib-Neo4j

The RDFLib-Neo4j library is a Python plugin that lets you use RDFLib’s API to parse RDF triples directly into a Neo4j graph database.  
Because Neo4j integrates well with the baseline RDFLib objects, which is also true for pyjelly, you can easily use both libraries.  

Install the following library:  

```bash
pip install rdflib-neo4j
```

For more information, visit the following references from the RDFLib-Neo4j original sources:

- [RDFLib-Neo4j (GitHub)](https://github.com/neo4j-labs/rdflib-neo4j)
- [Neo4j Labs: RDFLib-Neo4j](https://neo4j.com/labs/rdflib-neo4j/)

### Parsing data from a Jelly file into Neo4j

To parse data from a `.jelly` file into the Neo4j database use the following example:

{{ code_example('neo4j_integration/01_rdflib_neo4j_parse_grouped.py') }}

which parses the data from a `.jelly` file into your AuraDB database.  

Please make sure that you provide your own credentials to the AuraDB instance:

```python
AURA_DB_URI
AURA_DB_USERNAME
AURA_DB_PWD
```

## NetworkX

NetworkX is a Python package that represents networks as graphs and allows for their creation, manipulation, and analysis.
Due to its conversion utilities, it integrates seamlessly with RDFLib and pyjelly, so you can easily integrate these libraries.

Install the following libraries:

```bash
pip install networkx==3.2.1 matplotlib==3.9.4
```

To get more information, see the package's documentation and other useful utilities:

- [NetworkX examples](https://networkx.org/documentation/stable/auto_examples/index.html)
- [NetworkX repository (github)](https://github.com/networkx/networkx)
- [RDFLib external graph integration](https://rdflib.readthedocs.io/en/7.1.0/_modules/rdflib/extras/external_graph_libs.html)

In the following sub-sections, we will describe a few useful and baseline use cases for performing integration between the modules.

### Parse graph, visualize it, calculate useful graph-based characteristics

To load data into a NetworkX object, starting from a `.jelly` file, and (optionally) calculate some practical graph characteristics, see the example:

{{ code_example('networkx_integration/01_parse_calculate_visualize.py') }}

which loads the data from RDFLib graph into equivalent NetworkX graph, performs computation of useful graph theory metrics and visualizes the graph.

### Transform and serialize NetworkX graph

To transform a NetworkX graph into an RDFLib graph and perform its serialization into the `.jelly` format, look into the example:

{{ code_example('networkx_integration/02_serialize.py') }}

which converts an example-defined NetworkX graph into an RDFLib graph and performs its serialization.