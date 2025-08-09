import pathlib
import runpy
import urllib.request
from typing import IO

import pytest

SCRIPTS_DIR = (
    pathlib.Path(__file__).parent
    / pathlib.Path((pathlib.Path(__file__).parent / "examples").read_text().strip())
).resolve()
scripts = sorted(SCRIPTS_DIR.glob("*.py"))


@pytest.mark.parametrize("script", scripts, ids=lambda p: p.name)
def test_rdflib_examples(script: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Skip test
    if script.name == "03_parse_autodetect.py":
        pytest.skip(f"Skipping test {script.name}")
    # Run the examples in a temporary directory to avoid polluting the repository
    monkeypatch.chdir(pathlib.Path(__file__, "..", "temp").resolve())
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
