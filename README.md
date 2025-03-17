# pyjelly
# UNDER CONSTRUCTION, PROCEED WITH CAUTION!!!

## Protobuf in python dump:
- There are three separate implementations of Python Protobuf. All of them offer the same API and are thus functionally the same, though they have very different performance characteristics.
- upb (C-backed) is the fastest
- code generator in Python -- Python is a little different — the Python compiler generates a module with a static descriptor of each message type in your .proto, which is then used with a metaclass to create the necessary Python data access class at runtime.
- compiler version - protoc-30.0-linux-aarch_64.zip -- does not work, had to use homebrew version 29.3

## rdf.proto dump
- for plain .ttl file conversion to protobuf binary format
- start with RdfStreamFrame
- there will be RdfStreamRows
- the first RdfStreamRow should be RdfStreamOptions
- the rest will actually encode rdf content
- no explicit END sequence