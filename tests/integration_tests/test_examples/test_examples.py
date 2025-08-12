import importlib
import pathlib
import runpy
import urllib.request
from typing import IO

import pytest

BASE_DIR = pathlib.Path(__file__).parent


def resolve_scripts_dir(name: str) -> pathlib.Path:
    path = BASE_DIR / name
    if path.is_file():
        return (BASE_DIR / pathlib.Path(path.read_text().strip())).resolve()
    if path.is_dir():
        return path.resolve()
    raise FileNotFoundError(path)


SCRIPTS_RDFLIB = resolve_scripts_dir("examples/examples_rdflib")
SCRIPTS_GENERIC = resolve_scripts_dir("examples/examples_generic")
SCRIPTS_RDFLIB_CASE = ["rdflib/03_parse_autodetect.py"]

example_scripts = [
    pytest.param("rdflib", p, id=f"rdflib/{p.name}")
    for p in sorted(SCRIPTS_RDFLIB.glob("*.py"))
] + [
    pytest.param("generic", p, id=f"generic/{p.name}")
    for p in sorted(SCRIPTS_GENERIC.glob("*.py"))
]


@pytest.mark.parametrize(
    ("set_name", "script"), example_scripts, ids=lambda p: f"{p[0]}/{p[1].name}"
)
def test_examples(
    set_name: str, script: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    temp_dir = pathlib.Path(__file__, "..", "temp").resolve()

    if f"{set_name}/{script.name}" in SCRIPTS_RDFLIB_CASE:
        # required to mock __init__ for 03_parse_autodetect in order to work
        import pyjelly.options as _opts

        monkeypatch.setattr(_opts, "INTEGRATION_SIDE_EFFECTS", True, raising=False)
        pytest.importorskip("pyjelly.integrations.rdflib")
        import pyjelly.integrations.rdflib as _integration

        importlib.reload(_integration)

    # Run the examples in a temporary directory to avoid polluting the repository
    monkeypatch.chdir(temp_dir)
    # Mock HTTP requests to avoid network calls during tests
    monkeypatch.setattr(urllib.request, "urlopen", urlopen_mock)
    runpy.run_path(str(script))


def urlopen_mock(url: str) -> IO[bytes]:
    response_file = ""
    mode = "rb"
    if url.endswith(".jelly.gz"):
        response_file = "sample.jelly.gz"
    elif url.endswith(".jelly"):
        response_file = "sample.jelly"
    elif url.endswith(".gz"):
        response_file = "sample.nt.gz"
    else:
        response_file = "sample.nt"
        mode = "r"
    # ruff: noqa: PTH123
    return open("../example_data/" + response_file, mode=mode)
