# NetworkX

NetworkX is a Python package that represents complex networks as graphs and allows for their manipulation.

Install the following libraries:

```bash
pip install pyjelly[rdflib] networkx==3.2.1 matplotlib==3.9.4
```

Below there are few useful examples to follow.

## Parse graph, show it

To load graph form a `.jelly` file, and (optionally) calculate some practical graph characteristics, see:

{{ code_example('networkx_integration/01_parse_calculate_visualize.py') }}

which converts an RDFLib graph to NetworkX, computes key metrics and visualizes it.

## Serialize NetworkX graph

This example shows how to write a NetworkX graph to a Jelly file.:

{{ code_example('networkx_integration/02_serialize.py') }}

which converts NetworkX graph into an RDFLib insance and serializes it.

## Related sources

To get more information, see the following:

- [NetworkX examples](https://networkx.org/documentation/stable/auto_examples/index.html)
- [NetworkX repository (github)](https://github.com/networkx/networkx)
- [RDFLib external graph integration](https://rdflib.readthedocs.io/en/7.1.0/_modules/rdflib/extras/external_graph_libs.html)
