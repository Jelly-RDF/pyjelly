from __future__ import annotations

import shlex
import shutil
import subprocess
from functools import partial

import pytest

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
