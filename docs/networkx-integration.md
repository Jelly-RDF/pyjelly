# NetworkX

NetworkX is a Python package that represents complex networks as graphs and allows for their manipulation.

Install the following libraries:

```bash
pip install pyjelly[rdflib] networkx==3.2.1 matplotlib==3.9.4
```

Below there are few useful examples to follow.

## Parse graph, show it

Let's investigate relationships between worldwide political figures (support and opposition relations)!
We are given a graph in `.jelly` containing information about political stances extracted from news articles.  
Let's dive in and get some useful information!

We can easily load it:  

{{ code_example('networkx_integration/01_parse_calculate_visualize.py', 24, 27) }}

Output from `print()`:

```text
Loaded graph with 90000 instances.
```

Convert it into a convenient NetworkX graph:

{{ code_example('networkx_integration/01_parse_calculate_visualize.py', 55, 56) }}

Is our graph fully connected? It's important to know (are all political relations tied together?), let's check here:

{{ code_example('networkx_integration/01_parse_calculate_visualize.py', 58, 60) }}

Output from `print()`:

```text
Connected components: 24
```

Which nodes are connected the most (have most connections?), let's see top 5 of them:

{{ code_example('networkx_integration/01_parse_calculate_visualize.py', 62, 66) }}

Output from `print()`:

```text
Top 5 nodes sorted by degree:
Socrates: 241
Cavaco: 189
Passos Coelho: 187
Costa: 179
Antonio Costa: 168
```

What is the shortest path between two nodes? We can check:

{{ code_example('networkx_integration/01_parse_calculate_visualize.py', 75, 79) }}

Output from `print()`:

```text
Shortest path from Socrates to Obama: Socrates -> Marcelo Rebelo de Sousa -> Durao Barroso -> Obama
```

However, it's best to see the full picture (for our example we truncate to 10 nodes for clarity):

{{ code_example('networkx_integration/01_parse_calculate_visualize.py', 81, 92) }}

The graph presents as follows

<div style="text-align:center;">
  <img src="../assets/images/networkx_visualization_example.png" width="600" loading="lazy" alt="NetworkX visualization example" />
</div>


In summary:

{{ snippet_admonition('examples/networkx_integration/01_parse_calculate_visualize.py', 0, 95, title="Entire example", expanded=False) }}

We converted an RDFLib graph to NetworkX, calculated insightful metrics and visualized the graph.  

For more info about the data source please see the [Politiquices dataset in RiverBench](https://riverbench.github.io/v/2.1.0/datasets/politiquices/) and its [original source (Portuguese)](http://www.politiquices.pt/).

## Serialize NetworkX graph

This example shows how to write a NetworkX graph to a Jelly file.:

{{ code_example('networkx_integration/02_serialize.py') }}

Which converts the NetworkX graph into an RDFLib `Graph` instance and serializes it.

## Related sources

To get more information, see the following:

- [NetworkX examples](https://networkx.org/documentation/stable/auto_examples/index.html)
- [NetworkX repository (github)](https://github.com/networkx/networkx)
- [RDFLib external graph integration](https://rdflib.readthedocs.io/en/7.1.0/_modules/rdflib/extras/external_graph_libs.html)
