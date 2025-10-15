from setuptools import setup
from mypyc.build import mypycify

hot_paths = [
    "pyjelly/serialize/encode.py",
    # mypyc has A LOT of problems with this "pyjelly/serialize/flows.py",
    # "pyjelly/serialize/streams.py",
    # "pyjelly/serialize/lookup.py",
    # "pyjelly/parse/decode.py",
    # "pyjelly/parse/ioutils.py",
    # "pyjelly/parse/lookup.py",
    # "pyjelly/integrations/generic/parse.py",
    # "pyjelly/integrations/generic/serialize.py",
]

setup(
    name="pyjelly",
    packages=['pyjelly'],
    ext_modules=mypycify(
        [
            "--disallow-untyped-defs",
            "--no-warn-no-return",
            *hot_paths,
        ]
    ),
)
