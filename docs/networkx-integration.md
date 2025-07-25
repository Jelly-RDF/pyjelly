# NetworkX

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

## Parse graph, visualize it, calculate useful graph-based characteristics

To load data into a NetworkX object, starting from a `.jelly` file, and (optionally) calculate some practical graph characteristics, see the example:

{{ code_example('networkx_integration/01_parse_calculate_visualize.py') }}

which loads the data from RDFLib graph into equivalent NetworkX graph, performs computation of useful graph theory metrics and visualizes the graph.

## Transform and serialize NetworkX graph

To transform a NetworkX graph into an RDFLib graph and perform its serialization into the `.jelly` format, look into the example:

{{ code_example('networkx_integration/02_serialize.py') }}

which converts an example-defined NetworkX graph into an RDFLib graph and performs its serialization.