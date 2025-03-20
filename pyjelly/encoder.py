"""Pure-Python version of encoder lookup inspired by Jelly-JVM."""

from __future__ import annotations

from collections import OrderedDict
from typing import NamedTuple


class LookupEntry(NamedTuple):
    get_ident: int
    set_ident: int
    is_new: bool


class Lookup(OrderedDict): ...
