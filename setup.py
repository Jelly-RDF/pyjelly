from setuptools import setup, find_packages
from mypyc.build import mypycify
from pathlib import Path
import os
import sys

hot_paths = [
    "pyjelly/serialize/encode.py",
    "pyjelly/serialize/lookup.py",
    "pyjelly/serialize/streams.py",
    "pyjelly/serialize/ioutils.py",
    "pyjelly/integrations/generic/serialize.py",
    "pyjelly/integrations/generic/parse.py",
    "pyjelly/integrations/rdflib/serialize.py",
    "pyjelly/integrations/rdflib/parse.py",
    "pyjelly/parse/decode.py",
    "pyjelly/parse/ioutils.py",
    "pyjelly/parse/lookup.py",
]

missing = [p for p in hot_paths if not Path(p).exists()]
if missing:
    raise FileNotFoundError(f"mypyc hot paths not found: {missing}")

ext_modules = None
if (
    os.environ.get("PYJELLY_DISABLE_MYPYC") != "1"
    and sys.implementation.name == "cpython"
):
    ext_modules = mypycify(
        [
            "--ignore-missing-imports",
            "--disallow-untyped-defs",
            "--no-warn-no-return",
            "--disable-error-code=type-arg",
            "--disable-error-code=misc",
            *hot_paths,
        ]
    )

setup(
    name="pyjelly",
    packages=find_packages(exclude=("tests", "docs", "examples")),
    ext_modules=ext_modules,
)
