# pyjelly

> [!Warning]
> This project is in early development stage.

couple notes for an eager reviewer of this draft

**nothing present here is guaranteed to land in the PR.**
**it's going to be split up with partial staging and stacked**

## project breakdown
- `pyjelly` - the library src, check _module breakdown_ below
- `test.nt` - a very simple N-triples i work with, for now
- `z/`
  - files from serialization/deserialization to text; it's called `z` because it places on bottom of my explorer side bar
  - i use `delta` (like `diff`) for comparisons
- `jelly-inspect` - a simple script that does some statistics of frames and writes frames from a file
  - `jelly-cli from-jelly ...` has two issues that are bothering me for now (and i won't distract myself with itnow),
    so i stick into using my own script
    - it assumes a stream of triples in `test.nt` is a stream of quads, while riot doesn't
    - in the text format, frames aren't counted correctly (always 0)
- `scripts`
  - `compile-proto` - a simple script for compiling protobuf to runtime modules
other things are intuitive or python boilerplate

## module breakdown (`pyjelly`)

- `pb2` - protobuf-generated boilerplate with runtime protobuf objects
- `proto` - bundled proto spec from a submodule; its freshness is managed by dependabot with 1-day interval
- `consumers` - routines for consuming messages relying on context
- `contexts` - context classes corresponding to physical stream types, prob. can be simplified
- `producers` - routines for producing messages relying on context
- `stax` - logical stream types
- `readers` - for consumers - behavioral (validation) routines from the spec (e.g. wait for options first, validate options)
- `writers` - for producers - behavioral (validation) routines from the spec (e.g. send options first, validate options)
- `integrations` - integrations with other projects in/around/near the rdf ecosystem
  - `rdflib_plugin` - very early-stage playground to have a practical point of reference

## why is the only serialization logic (non-generic!) in `pyjelly.integrations.rdflib_plugin`

abstraction will grow from the obscure details. we only implement flat triples with a completely random
"should-flush" strategy, but we're planning on figuring out
- a way of determining what should the physical stream type of a serialized rdflib graph be, maybe via `serialize(**args)`
- defaults to stream options (i stole the stream options from a sample file serialized by jelly-jvm)
- ofc that long function will become an emergent set of separate components

## the reason `consumers` and `producers` functions don't belong to the `StreamContext` class

practical.

1. `StreamContext` exposing both `consumers` and `producers` API imposes relying on both,
   rendering `StreamContext` responsible for more than one job
2. a different way of separating consumers and producer interfaces is with mixins.
   mixins are OK, but i'm not enthusiastic because that still doesn't offer as much elasticity
3. routines in `consumers` and `producers` are final--inheritance with method-based API 
   doesn't offer any advantage
4. easier mocking

## what are `writers` and `readers` for

jelly has some behavioral semantics that _has_ to be preserved.
i'm not yet sure about the structure, and that one is probably going to be class-based instead of
function-based, because you typically may want to plug in to some behaviors/override some of them.

an example of these behavioral semantics is always sending stream options at the beginning of the 
stream.

a different idea is to rely on lifespan context managers, a pattern like in fastapi.

that's TBC.

