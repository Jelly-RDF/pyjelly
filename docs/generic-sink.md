# Generic API

This guide explains how to use pyjelly’s **generic API** to write and read RDF statements into the Jelly format without any external library.

## Installation

Install pyjelly from PyPI:

```bash
pip install pyjelly
```

### Requirements

- Python 3.9 or newer  
- Linux, macOS, or Windows

## Usage without external libraries

Unlike the example in [getting started](getting-started.md), the Generic API does not use the RDFLib or any other third-party libraries, but it works in much the same way.

## Serializing statements to a Jelly file

To make a set of triples/quads and write them to a Jelly file, use:

{{ code_example('generic/01_serialize.py') }}

This example uses pyjelly’s simple custom triple/quad type, which is easy to create and work with.

## Parsing statements from a Jelly file

To load triples/quads into your python object from a `.jelly` file, see:

{{ code_example('generic/02_parse.py') }}

Which retrieves data from your `.jelly` file.

### Parsing a stream of graphs

Similarly, to process a Jelly stream as a stream of graphs through generic API, see:

{{ code_example('generic/06_parse_grouped.py') }}

Where we use a [dataset of weather measurements](https://w3id.org/riverbench/datasets/lod-katrina/dev) and count the number of triples in each graph.

### Parsing a stream of statements

You can also process a Jelly stream as a flat stream with only generic API:

We look through a fragment of Denmark's OpenStreetMap to find all city names:

{{ code_example('generic/07_parse_flat.py') }}

We get a generator of stream events, which allows us to process the file statement-by-statement, however with no external libraries used.

## Streaming data

If you need to process a certain quantity of statements both efficiently and iteratively, you can provide a simple generator:

{{ code_example('generic/03_streaming.py') }}

With this method you avoid storing all statements in memory, which greatly improves performance.

### Serializing a stream of graphs

If you have a generator object containing graphs, you can use a generic approach for serialization:

{{ code_example('generic/04_serialize_grouped.py')}}

Grouped data is streamed in its original form, no need for additional RDF libraries like RDFLib. 

### Serializing a stream of statements

Serializing a generator object of statements to `.jelly` file through generic API:

{{ code_example('generic/05_serialize_flat.py')}}

Data is transmitted and kept ordered and simple.

### Working with byte buffers and Kafka

When working with Kafka or other message brokers, you may want to write Jelly data to a byte buffer instead of a file. You can do this by using the `BytesIO` class from the `io` module:

{{ code_example('generic/08_byte_buffer.py')}}

The `data` variable is of type `bytes`, and can be passed to Kafka with [`KafkaProducer.send(value=data)`](https://kafka-python.readthedocs.io/en/master/), or any other API that accepts byte buffers. Same trick may be used when working with the [RDFLib integration](getting-started.md).

When working with Kafka, you should be aware of the broker's offset management and partitioning strategies. Data within one Jelly stream must be strictly ordered and no frames may be dropped. If you have less strict ordering guarantees, you should split up the stream into multiple Jelly streams, each with guaranteed consistency.

### See also

If you are familiar with RDFLib, you can use pyjelly together with RDFLib in a similar way. [See the dedicated guide](getting-started.md).

