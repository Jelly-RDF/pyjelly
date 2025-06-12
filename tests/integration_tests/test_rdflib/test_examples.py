import pathlib
import runpy

import pytest

scripts = list(pathlib.Path(__file__, "..", "examples").resolve().glob("*.py"))
scripts.sort()


@pytest.mark.parametrize("script", scripts, ids=lambda p: p.name)
def test_rdflib_examples(script: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Run the examples in a temporary directory to avoid polluting the repository
    monkeypatch.chdir(pathlib.Path(__file__, "..", "temp").resolve())
    runpy.run_path(str(script))
