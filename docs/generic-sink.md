# Generic API
This guide explains how to use pyjelly’s **Generic API** to write and read RDF triples into the Jelly format without requiring any external library.

## Installation

Install pyjelly from PyPI:

```bash
pip install pyjelly
```

### Requirements

- Python 3.9 or newer  
- Linux, macOS, or Windows

## Usage without external libraries
Because we don't rely on any external libraries, using generic API looks slightly different, however keeps the same functionality.

## Writing Triples to a Jelly file

To create a set of triples and write them to a Jelly file, you use:

{{ code_example('generic/01_serialize.py') }}

Which allows for serialization with no need for extra dependencies.

## Reading Triples from a Jelly file

To load triples into your python object from a `.jelly` file, see:

{{ code_example('generic/02_parse.py') }}

Which will retrieve data from your previously saved file.

### Parsing a stream of graphs

Similarly, to process a Jelly stream as a stream of graphs through core API, see:

{{ code_example('generic/06_parse_grouped.py') }}

Where we use a [dataset of weather measurements](https://w3id.org/riverbench/datasets/lod-katrina/dev) and count the number of triples in each graph.

### Parsing a stream of triples

You can also process a Jelly stream as a flat stream with only generic API:

We look through a fragment of Denmark's OpenStreetMap to find all city names:

{{ code_example('generic/07_parse_flat.py') }}

We are also yielded a generator of stream events, which allows us to process the file triple-by-triple, however with no external libraries used.

## Streaming large data

If you need to process a large file/quantity of triples, you can provide a simple generator to iterate through data efficiently:

{{ code_example('generic/03_streaming.py') }}

Through this method you don't store every statement in memory, greatly improving its performance.

### Serializing a stream of graphs

If you have a generator object containing graphs, you can use generic approach of serialization: 

{{ code_example('generic/04_serialize_grouped.py')}}

Without using external RDF dependencies, grouped data is streamed preserving their original division. 

### Serializing a stream of statements

Serializing a generator object of statements to `.jelly` through generic API:

{{ code_example('generic/05_serialize_flat.py')}}

Data is transmitted and kept ordered and simple. 