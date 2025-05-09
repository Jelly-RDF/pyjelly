from __future__ import annotations

import sys

import rdflib

from pyjelly import options

# No need for protoletariat ;)
sys.path.append("pyjelly/_pb2/")

options.INTEGRATION_SIDE_EFFECTS = False
rdflib.NORMALIZE_LITERALS = False
