from __future__ import annotations

import io
from pathlib import Path

import pytest
from rdflib import Dataset, Graph
from itertools import islice


def pytest_addoption(parser: pytest.Parser) -> None:
    g = parser.getgroup("benchmark")
    g.addoption("--in-nt", type=str, help="path to N-Triples file.")
    g.addoption("--in-nq", type=str, help="path to N-Quads file.")
    g.addoption(
        "--in-jelly-triples",
        type=str,
        default=None,
        help="optional Jelly triples file; if none, generated in-memory from nt file.",
    )
    g.addoption(
        "--in-jelly-quads",
        type=str,
        default=None,
        help="optional Jelly quads file; if none, generated in-memory from nq slice.",
    )

    g.addoption(
        "--limit-statements",
        type=int,
        default=5_000_000,
        help="first N statements from input.",
    )
    g.addoption(
        "--warmup-rounds",
        type=int,
        default=5,
        help="warmup rounds, not counted to evaluation.",
    )
    g.addoption("--rounds", type=int, default=10, help="measured rounds.")
    g.addoption("--iterations", type=int, default=1, help="iterations per round.")


def _slice_lines_to_bytes(path: Path, limit: int) -> bytes:
    buf = io.BytesIO()
    with path.open("rb") as f:
        buf.writelines(islice(f, limit))
    return buf.getvalue()


@pytest.fixture(scope="session")
def limit_statements(request: pytest.FixtureRequest) -> int:
    return int(request.config.getoption("--limit-statements"))


@pytest.fixture(scope="session")
def pedantic_cfg(request: pytest.FixtureRequest) -> dict[str, int]:
    return {
        "warmup_rounds": int(request.config.getoption("--warmup-rounds")),
        "rounds": int(request.config.getoption("--rounds")),
        "iterations": int(request.config.getoption("--iterations")),
    }


@pytest.fixture(scope="session")
def nt_path(request: pytest.FixtureRequest) -> Path:
    opt = request.config.getoption("--in-nt")
    assert opt, "--in-nt is required"
    p = Path(opt)
    assert p.exists(), f"--in-nt not found: {p}"
    return p


@pytest.fixture(scope="session")
def nq_path(request: pytest.FixtureRequest) -> Path:
    opt = request.config.getoption("--in-nq")
    assert opt, "--in-nq is required"
    p = Path(opt)
    assert p.exists(), f"--in-nq not found: {p}"
    return p


@pytest.fixture(scope="session")
def jelly_triples_path(request: pytest.FixtureRequest) -> Path | None:
    opt = request.config.getoption("--in-jelly-triples")
    return Path(opt) if opt else None


@pytest.fixture(scope="session")
def jelly_quads_path(request: pytest.FixtureRequest) -> Path | None:
    opt = request.config.getoption("--in-jelly-quads")
    return Path(opt) if opt else None


@pytest.fixture(scope="session")
def nt_bytes_sliced(nt_path: Path, limit_statements: int) -> bytes:
    return _slice_lines_to_bytes(nt_path, limit_statements)


@pytest.fixture(scope="session")
def nq_bytes_sliced(nq_path: Path, limit_statements: int) -> bytes:
    return _slice_lines_to_bytes(nq_path, limit_statements)


@pytest.fixture(scope="session")
def nt_graph(nt_bytes_sliced: bytes) -> Graph:
    g = Graph()
    g.parse(data=nt_bytes_sliced, format="nt")
    return g


@pytest.fixture(scope="session")
def nq_dataset(nq_bytes_sliced: bytes) -> Dataset:
    ds = Dataset()
    ds.parse(data=nq_bytes_sliced, format="nquads")
    return ds


@pytest.fixture(scope="session")
def jelly_triples_bytes(jelly_triples_path: Path | None, nt_graph: Graph) -> bytes:
    if jelly_triples_path and jelly_triples_path.exists():
        return jelly_triples_path.read_bytes()
    return nt_graph.serialize(destination=None, format="jelly", encoding="utf-8")


@pytest.fixture(scope="session")
def jelly_quads_bytes(jelly_quads_path: Path | None, nq_dataset: Dataset) -> bytes:
    if jelly_quads_path and jelly_quads_path.exists():
        return jelly_quads_path.read_bytes()
    return nq_dataset.serialize(destination=None, format="jelly", encoding="utf-8")


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "benchmark: flat ser/des benchmarks")
    config.addinivalue_line(
        "markers", "triples: triples-only benchmarks (NT/Jelly-triples)"
    )
    config.addinivalue_line("markers", "quads: quads-only benchmarks (NQ/Jelly-quads)")


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    has_nt = bool(config.getoption("--in-nt"))
    has_nq = bool(config.getoption("--in-nq"))

    deselected: list[pytest.Item] = []
    selected: list[pytest.Item] = []

    for it in items:
        is_triples = it.get_closest_marker("triples") is not None
        is_quads = it.get_closest_marker("quads") is not None

        if is_triples and not has_nt:
            deselected.append(it)
            continue
        if is_quads and not has_nq:
            deselected.append(it)
            continue

        selected.append(it)

    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = selected
