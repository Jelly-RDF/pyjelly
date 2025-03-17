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

## TODO:
1) figure out look up tables (done) and ids :(((
2) why RdfNameEntry in namespace declarations
3) make proper checks if fields are present in the message
4) how to handle rdf* in python?
5) what do we do if a full IRI is passed to subject despite using prefixes in the rest of the file? -- technically it would be nice to introduce a prefix ourselves? if not -- how to encode it? 
