# Benchmarks

Throughput and size benchmarks of Jelly-RDF in pyjelly, compared with the RDF formats of rdflib.

The method follows the [Jelly-JVM benchmarks](https://github.com/Jelly-RDF/jelly-jvm/tree/main/jmh) as closely as Python allows, so that the results can be read the same way. Differences are listed below.

## Running

Install the dependencies and download the datasets (about 400 MB, saved in `benchmarks/data/`):

```bash
uv sync --group benchmark
uv run python -m benchmarks fetch
```

Measure write (serialization) and read (parsing) throughput. Each command prints a table and saves the results as JSON in `benchmarks/results/`:

```bash
uv run python -m benchmarks run write
uv run python -m benchmarks run read
```

Measure serialized sizes, plain and compressed:

```bash
uv run python -m benchmarks size
```

All commands take `--datasets` and `--methods` (comma-separated), and `--rows`. Run `python -m benchmarks <command> --help` for the rest. Print saved results again with `python -m benchmarks report <file>...`.

A full run of all datasets and methods takes a few hours. For a quick check, use fewer datasets, sweeps and iterations:

```bash
uv run python -m benchmarks run read --datasets assist-iot-weather,nanopubs --sweeps 2 --warmup 1 --iterations 2 --time 0.5
```

### Getting stable results

- Use a recent CPython (3.14 for the latest results) and check that pyjelly is compiled with mypyc. The saved results say so in `pyjelly_compiled_with_mypyc`. An editable install compiles it, but the compiled modules are only for the Python version that built them.
- On a CPU with performance and efficiency cores, pin the benchmark to the performance cores. On the NVIDIA DGX Spark, we use cores 15–19: `taskset -c 15-19 uv run python -m benchmarks run read`.
- Don't run anything else heavy at the same time.

### Comparing Python versions

An editable install keeps pyjelly's compiled modules in the repository, and building for one Python version removes those of another. To run several versions side by side, install pyjelly as a wheel in each environment, and run the benchmarks from a folder that has only a link to `benchmarks/`, so that Python doesn't import pyjelly's sources from the repository instead of the wheel:

```bash
UV_PROJECT_ENVIRONMENT=/tmp/venv315 uv sync -p 3.15 --group benchmark --no-editable
mkdir -p /tmp/runner && ln -sfn "$PWD/benchmarks" /tmp/runner/benchmarks
cd /tmp/runner && /tmp/venv315/bin/python -m benchmarks run read
```

Building the wheel needs the `submodules/protobuf` submodule: `git submodule update --init`.

### Profiling one case

`case` runs one task, dataset and method in the current process. With `--profile`, the measured iterations run under cProfile, the top functions are printed, and the stats are saved for tools like `snakeviz`:

```bash
uv run python -m benchmarks case read nanopubs rdflib-jelly-stream --warmup 1 --iterations 1 --profile read.prof
```

cProfile doesn't see inside functions compiled with mypyc: their time is counted in the Python function that called them. It also slows down pure Python code much more than compiled code, so use it to see where the time goes, not to compare methods.

## Method

### Data

- 13 [RiverBench](https://w3id.org/riverbench) datasets, the same as in Jelly-JVM: all except `politiquices` (too short) and `yago-annotated-facts` (RDF-star, which most formats can't write).
- The first 100,000 statements of each dataset, read from its Jelly distribution with pyjelly. The order and the duplicates are kept.
- Before measuring, statements are kept in memory as a Python list, in the terms of the library that is measured: rdflib terms, or the generic integration's own terms. Making this list isn't measured.

### Methods

Method names are `<terms>-<format>`, plus `-<API>` for Jelly, which has two APIs. `<terms>` is the kind of term objects the statements are made of: rdflib's, or the generic ones of pyjelly's generic integration.

| Method | Writes | Reads |
|---|---|---|
| `rdflib-nt` | N-Triples / N-Quads, line by line from the list | rdflib's parser |
| `rdflib-turtle` | Turtle / TriG, from a Graph / Dataset | rdflib's parser |
| `rdflib-xml` | RDF/XML, from a Graph (triples only) | rdflib's parser |
| `rdflib-jsonld` | JSON-LD, from a Graph / Dataset | rdflib's parser |
| `rdflib-jelly-graph` | Jelly through rdflib's plugin API: `Graph.serialize` | `Graph.parse` |
| `rdflib-jelly-stream` | Jelly with pyjelly's flat streaming API, from the list | `parse_jelly_flat` |
| `generic-jelly-stream` | The same, with the generic integration's terms instead of rdflib's | `parse_jelly_flat` |

All Jelly methods use pyjelly's default options: the "big" lookup preset (4000 names, 150 prefixes, 32 datatypes) and frames of 250 rows. The other formats' writers get the dataset's prefixes.

### Tasks

- **Write**: the statements go from the list (or the graph, see below) to the writer, and the output to a stream that throws it away (like Java's `OutputStream.nullOutputStream()`). There's no extra buffering. Throughput counts the statements the writer writes: all 100,000 for writers that take the list.
- **Read**: the parser reads bytes from memory and gives statements back, which are only counted. Throughput counts the statements actually in the bytes: a writer that drops duplicates leaves fewer to read.
- **Size**: bytes of the written output, plain, with gzip (level 6) and with zstd (level 3), divided by 100,000 statements.

rdflib can only write Turtle, RDF/XML, JSON-LD and Jelly (through its plugin API) from a whole Graph or Dataset. The graph is built from the list before measuring, with the dataset's prefixes, so only `serialize` is measured. A graph has no duplicates, so these writers write fewer statements (e.g., 70,468 of 100,000 in assist-iot-weather), and their throughput counts only those. They also write statements in the graph's own order, not the dataset's. This differs from Jelly-JVM, which measures filling the graph too.

rdflib's parsers can only add statements to a Graph or Dataset. To measure only the parser, the graph uses a store that just counts statements. What's left of the graph's work is the type checks in `Graph.add`, about 2–5% of the time.

Before measuring, each case writes the data, reads it back and compares the statements (as sets for writers that drop duplicates, blank node labels ignored). A difference is printed as `ROUND-TRIP WARNING`. A method that can't read its own output is skipped.

### Measurement

- Each case (task, dataset, method) runs in its own Python process, like a JMH fork.
- In the process: 5 warmup iterations, then 5 measured iterations. An iteration repeats the operation until 1 second has passed, and always finishes the last operation. The case's result is the mean throughput of the measured iterations.
- Before each iteration, the garbage collector runs, outside the timing. The data prepared before measuring is frozen with `gc.freeze()`, so the collector doesn't go through it again and again during the measurement.
- A sweep runs every case once. There are 3 sweeps. The result is the median of the sweeps. Values whose sweeps differ by more than 10% are marked with `*`.
- Averages over datasets are geometric means. A method that couldn't run on all datasets has the number of datasets it ran on next to its mean.

### Differences from Jelly-JVM

- Parsers and writers can't be set to skip checks the way Jena and RDF4J can. For example, rdflib always checks IRIs when making a `URIRef`, which is part of every read with rdflib terms.
- Jelly-JVM 4 uses frames of 1024 rows, pyjelly 250. Both use their library's default.
- Jelly-JVM 4 writes Jelly-RDF 1.2 streams (protocol version 3, the column layout). pyjelly doesn't support Jelly-RDF 1.2 yet: it writes version 1, or version 2 when the stream has namespace declarations. So the Jelly results here are for the older format, which Jelly-JVM's results show to be bigger and slower to read than 1.2. The saved results record the highest version pyjelly supports in `pyjelly_max_jelly_version`.
