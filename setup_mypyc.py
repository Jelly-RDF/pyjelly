from setuptools import setup, find_packages
from mypyc.build import mypycify

hot_paths = [
    "pyjelly/serialize/encode.py",
    "pyjelly/serialize/lookup.py",
    "pyjelly/serialize/streams.py",
    "pyjelly/serialize/ioutils.py",

    "pyjelly/integrations/generic/serialize.py",
    "pyjelly/integrations/generic/parse.py",
    "pyjelly/integrations/rdflib/serialize.py",
    "pyjelly/integrations/rdflib /parse.py",

    "pyjelly/parse/decode.py",
    "pyjelly/parse/ioutils.py",
    "pyjelly/parse/lookup.py",   
]

setup(
    name="pyjelly",
    packages=find_packages(exclude=("tests", "docs", "examples")),
    ext_modules=mypycify(
        [   
            "--ignore-missing-imports",
            "--disallow-untyped-defs",
            "--no-warn-no-return",
            "--disable-error-code=type-arg",
            "--disable-error-code=misc",
            *hot_paths,
        ]
    ),
)
