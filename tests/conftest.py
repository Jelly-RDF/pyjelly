from __future__ import annotations

import sys

import pytest
import rdflib

from pyjelly import options

# No need for protoletariat ;)
sys.path.append("pyjelly/_pb2/")

options.INTEGRATION_SIDE_EFFECTS = False
rdflib.NORMALIZE_LITERALS = False


@pytest.fixture(autouse=True)
def make_output_dir() -> None:
    """Create the output directory if it doesn't exist."""
    from tests.meta import TEST_OUTPUTS_DIR

    TEST_OUTPUTS_DIR.mkdir(exist_ok=True)
