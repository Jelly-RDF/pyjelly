from __future__ import annotations

import shlex
import shutil
import subprocess
from enum import Enum
from functools import partial
from pathlib import Path

import pytest

# TODO(Piotr): Remove this list once the failing test cases are fixed.
# See https://github.com/Jelly-RDF/pyjelly/issues/145
failing_test_cases = [
    "to_jelly_triples_rdf_1_1_pos_014",
]

JELLY_CLI = shutil.which("jelly-cli")

needs_jelly_cli = pytest.mark.skipif(
    not JELLY_CLI,
    reason="jelly-cli not found in PATH",
)


class JellyCLIError(Exception):
    """Exception raised when jelly-cli command fails."""


def jelly_cli(*args: object, hint: str | None = None) -> bytes:
    assert JELLY_CLI
    shell_args = [JELLY_CLI, *map(str, args)]
    try:
        return subprocess.check_output(shell_args, stderr=subprocess.STDOUT)  # noqa: S603 internal use
    except subprocess.CalledProcessError as error:
        command = shlex.join(shell_args)
        note = f"Command: {command}"
        if hint:
            note += f"\nHint: {hint}"
        raise JellyCLIError(error.output.decode() + "\n" + note) from None


jelly_validate = partial(jelly_cli, "rdf", "validate")


def id_from_path(path: Path) -> str:
    return f"{path.parent.parent.name}_{path.parent.name}_{path.name}"


class PhysicalTypeTestCasesDir(str, Enum):
    TRIPLES = "triples_rdf_1_1"
    QUADS = "quads_rdf_1_1"
    GRAPHS = "graphs_rdf_1_1"

    def __str__(self) -> str:
        return self.value


class GeneralizedTestCasesDir(str, Enum):
    TRIPLES = "triples_rdf_1_1_generalized"
    QUADS = "quads_rdf_1_1_generalized"
    GRAPHS = "graphs_rdf_1_1_generalized"

    def __str__(self) -> str:
        return self.value


class RDFStarGeneralizedTestCasesDir(str, Enum):
    TRIPLES = "triples_rdf_star_generalized"
    QUADS = "quads_rdf_star_generalized"
    GRAPHS = "graphs_rdf_star_generalized"

    def __str__(self) -> str:
        return self.value


class RDFStarTestCasesDir(str, Enum):
    TRIPLES = "triples_rdf_star"
    QUADS = "quads_rdf_star"
    GRAPHS = "graphs_rdf_star"

    def __str__(self) -> str:
        return self.value


def walk_directories(
    *dirs: str | Path,
    glob: str | None = None,
) -> pytest.MarkDecorator:
    paths: list[Path] = []

    for directory in map(Path, dirs):
        if not directory.is_dir():
            # a warning here, albeit potentially helpful, is too noisy in practice
            continue
        paths.extend(directory.glob(glob or "*"))

    paths = [path for path in paths if id_from_path(path) not in failing_test_cases]
    if not paths:
        return pytest.mark.parametrize("path", [None], ids=["no-tests-found"])

    return pytest.mark.parametrize("path", paths, ids=id_from_path)
