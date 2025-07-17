# Pyjelly integration with external libraries

This section describes how to integrate useful third‑party libraries that utilize RDFLib.  
In order to proceed with the content of the document, one should be acquainted with the [getting-started.md](getting-started.md) file.

## Neo4j - rdflib

The Neo4j RDFLib library is a Python plugin that lets you use RDFLib’s API to parse RDF triples directly in a Neo4j graph database.  
Because Neo4j integrates well with the baseline RDFLib objects, which is also true for pyjelly, both libraries can be easily combined.  

In further steps we will exemplify the integration, however first, (in our pyjelly environment) we should install rdflib-neo4j:  
```
pip install rdflib-neo4j
```

For more information, one should visit the following references found on the rdflib-neo4j original sources, which formed the base of
the following document:
    - [github.com/neo4j-labs/rdflib-neo4j](https://github.com/neo4j-labs/rdflib-neo4j)
    - [neo4j.com/labs/rdflib-neo4j/](https://neo4j.com/labs/rdflib-neo4j/)

### Configure Your AuraDB Connection

Before running the example code, make sure to provide your own credentials to the AuraDB instance listed below:
```
AURA_DB_URI
AURA_DB_USERNAME
AURA_DB_PWD
```

### Parsing data into neo4j from a jelly file

To parse data into the neo4j database from `.jelly` file use the following example:

{{ code_example('neo4j_integration/01_rdflib_neo4j_parse.py') }}

which parses the data from `.jelly` file into the user's AuraDB database.

## NetworkX

NetworkX is a Python package for the creation, manipulation, and analysis of the structure, dynamics, and functions of networks represented as graphs.
Due to its seamless integration RDFlib and PyJelly via its conversion utilities, these libraries can be easily combined for graph analysis and transformation tasks.

To successfully go through the use cases, one should primarily install the following libraries:
```
pip install networkx==3.2.1 matplotlib==3.9.4
```

To get more information about the NetworkX itself, see the package's documentation and how it integrates with RDFLib, and thus also PyJelly, please visit the following websites:
    - [networkx.org/documentation/stable/auto_examples](https://networkx.org/documentation/stable/auto_examples/index.html)
    - [rdflib.readthedocs.io/en/7.1.0/_modules/rdflib/extras/external_graph_libs.html](https://rdflib.readthedocs.io/en/7.1.0/_modules/rdflib/extras/external_graph_libs.html)
    - [github.com/networkx/networkx](https://github.com/networkx/networkx)

In the following sub-sections, we will describe a few useful and baseline use cases for performing integration between the modules.

### Parse graph into NetworkX object and visualizing it

To load data into a networkx object, starting from a `.jelly` file, one can look into the example:

{{ code_example('networkx_integration/01_parse_visualize.py') }}

which loads the data from RDFLib graph into equivalent networkx graph, and performs its visualization.

### Calculating useful graph-based characteristics via NetworkX library

NetworkX allows for advanced, thorough and easy computation of important metrics from graph theory, which are also useful for KG.

In the example {{ code_example('networkx_integration/02_parse_calculate.py') }}

we demonstrate how to easily calculate the number of connected components and top 5 nodes sorted by degree.

For more useful functions, attributes provided by the NetworkX library, please visit the aforementioned documentation.

### Transform and serialize NetworkX graph

To transform a NetworkX graph into RDFLib graph and perform its serialization into the `.jelly` format, look into the example:

{{ code_example('networkx_integration/03_serialize.py') }}

Which converts an example-defined NetworkX graph into RDFLib graph and performs its serialization.