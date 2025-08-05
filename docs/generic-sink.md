# Generic Interface
This guide explains how to use pyjelly’s **Generic Interface** to write and read RDF triples into the Jelly format without requiring any external RDF library.

## Installation (no RDFLib)

Install pyjelly from PyPI:

```bash
pip install pyjelly
```

### Requirements

- Python 3.9 or newer  
- Linux, macOS, or Windows

## Usage without external libraries
Because we don't rely on any external libraries, integration through generic interface looks different however keeps the same functionality.

## Writing Triples to a Jelly File

To create a set of triples and write them to a Jelly, you use:

{{ code_example('generic/01_serialize.py') }}

Which allows for serialization with no need for RDFLib.

## Reading Triples from a Jelly File

To load triples into your python object from a `.jelly` file, see:

{{ code_example('generic/02_parse.py') }}

Which will retrieve data from your previously saved file.

## Streaming large data

If you need to process a large file/quantity of triples, you can provide a simple generator to iterate through data efficiently:

{{ code_example('generic/03_streaming.py') }}

Through this method you don't store every statement in memory, greatly improving its performance.

