import shlex
import shutil
import subprocess
from collections.abc import Callable, Iterator
from functools import partial
from itertools import chain
from pathlib import Path

import pytest

JELLY_CLI = shutil.which("jelly-cli")

needs_jelly_cli = pytest.mark.skipif(
    not JELLY_CLI,
    reason="jelly-cli not found in PATH",
)


class JellyCLIError(Exception):
    """Exception raised when jelly-cli command fails."""


def jelly_cli(*args: object) -> bytes:
    assert JELLY_CLI
    shell_args = [JELLY_CLI, *map(str, args)]
    try:
        return subprocess.check_output(shell_args, stderr=subprocess.STDOUT)  # noqa: S603 internal use
    except subprocess.CalledProcessError as error:
        command = shlex.join(shell_args)
        note = f"Command: {command}"
        raise JellyCLIError(error.output.decode() + "\n" + note) from None


jelly_validate = partial(jelly_cli, "rdf", "validate")


def id_from_path(path: Path) -> str:
    base = f"{path.parent.parent.name}_{path.parent.name}_{path.name}"
    return base.replace("rdf_1_1_", "")


def physical_types_glob(pattern: str, base: Path) -> Iterator[Path]:
    # Adheres to the structure described in conformance_tests/rdf/README.md
    return chain(
        (base / "triples_rdf_1_1").glob(pattern),
        (base / "quads_rdf_1_1").glob(pattern),
        (base / "graphs_rdf_1_1").glob(pattern),
    )


positive_glob = partial(physical_types_glob, "pos_*")
negative_glob = partial(physical_types_glob, "neg_*")


def test_cases_decorator(
    glob_func: Callable[[Path], Iterator[Path]],
    path: Path,
) -> pytest.MarkDecorator:
    return pytest.mark.parametrize("path", glob_func(path), ids=id_from_path)


positive_test_cases_for = partial(test_cases_decorator, positive_glob)
negative_test_cases_for = partial(test_cases_decorator, negative_glob)
