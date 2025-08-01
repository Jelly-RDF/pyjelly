# NetworkX

NetworkX is a Python package that represents complex networks as graphs and allows for their manipulation.

Install the following libraries:

```bash
pip install pyjelly[rdflib] networkx==3.2.1 matplotlib==3.9.4
```

Below there are few useful examples to follow.

## Parse graph, show it

Let's investigate worldwide political connections (support and oposition relations)!
We are given a graph in `.jelly` containing information about political stances extracted from news articles.  
Let's dive in and get some useful information!

We can easily load it:  

{{ snippet_admonition('examples/networkx_integration/01_parse_calculate_visualize.py', 17, 20, title="Loading") }}

Convert it into a convenient NetworkX graph:

{{ snippet_admonition('examples/networkx_integration/01_parse_calculate_visualize.py', 22, 23, title="Converting") }}

Is our graph fully connected? It's important to know (are all political relations tied togehter?), let's check here:

{{ snippet_admonition('examples/networkx_integration/01_parse_calculate_visualize.py', 25, 27, title="Fully connected check") }}


Which nodes are connected the most (have most connections?), let's see top 5 of them:

{{ snippet_admonition('examples/networkx_integration/01_parse_calculate_visualize.py', 29, 33, title="Top 5 nodes by degree") }}

What is the shortest path between two nodes? We can check:

{{ snippet_admonition('examples/networkx_integration/01_parse_calculate_visualize.py', 38, 42, title="Shortest path") }}

However afterall, its best to the full picture (for our example we truncate to 15 nodes for clarity):

{{ snippet_admonition('examples/networkx_integration/01_parse_calculate_visualize.py', 38, 42, title="Visualize graph") }}

The graph presents as follows

<div style="text-align:center;">
  <img src="../assets/images/networkx_visualization_example.png" width="600" loading="lazy" alt="NetworkX visualization example" />
</div>


In summary:

{{ snippet_admonition('examples/networkx_integration/01_parse_calculate_visualize.py', 0, 57, title="Entire example", expanded=False) }}

we converted an RDFLib graph to NetworkX, calculated insightful metrics and visualized the graph.  

For more info about the data source please see:

-[Politiquices graph dataset](https://riverbench.github.io/v/2.1.0/datasets/politiquices/)

## Serialize NetworkX graph

This example shows how to write a NetworkX graph to a Jelly file.:

{{ snippet_admonition('examples/networkx_integration/02_serialize.py', 0, 29, title="Serialization") }}

which converts NetworkX graph into an RDFLib insance and serializes it.

## Related sources

To get more information, see the following:

- [NetworkX examples](https://networkx.org/documentation/stable/auto_examples/index.html)
- [NetworkX repository (github)](https://github.com/networkx/networkx)
- [RDFLib external graph integration](https://rdflib.readthedocs.io/en/7.1.0/_modules/rdflib/extras/external_graph_libs.html)
