from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from hashlib import sha1
from pathlib import Path
from typing import IO, Any

from pyjelly.integrations.generic.generic_sink import (
    IRI,
    BlankNode,
    GenericStatementSink,
    Literal,
    Triple,
)


@dataclass
class KgtkConversionOptions:
    include_id: bool = False
    id_strategy: str = "counter"
    write_prefix_expansions: bool = True
    escape_tabs_newlines: bool = True


class KGTKJellyParser:
    def parse(
        self,
        source: str | Path | IO[bytes],
        sink: IO[str],
        *,
        options: KgtkConversionOptions | None = None,
    ) -> None:
        opts = options or KgtkConversionOptions()
        inp = self._open_input(source)
        try:
            g = GenericStatementSink()
            g.parse(inp)
            self._write_kgtk(g, sink, opts)
        finally:
            if isinstance(source, (str, Path)) and hasattr(inp, "close"):
                inp.close()

    @staticmethod
    def _open_input(source: str | Path | IO[bytes]) -> IO[bytes]:
        if isinstance(source, (str, Path)):
            return open(source, "rb")
        if hasattr(source, "read"):
            return source
        msg = "source must be a path or a binary IO stream"
        raise TypeError(msg)

    @staticmethod
    def _split_iri(iri: str) -> tuple[str, str]:
        for sep in ("#", "/"):
            i = iri.rfind(sep)
            if i != -1:
                return iri[: i + 1], iri[i + 1 :]
        return iri, ""

    @staticmethod
    def _quote_double(s: str, escape: bool) -> str:
        if escape:
            s = s.replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")
        return s

    @staticmethod
    def _quote_lang(s: str, lang: str, escape: bool) -> str:
        if escape:
            s = s.replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")
        return s

    @staticmethod
    def _is_int_dt(dt: str) -> bool:
        return dt.endswith(
            (
                "#integer",
                "#int",
                "#long",
                "#short",
                "#byte",
                "#nonNegativeInteger",
                "#positiveInteger",
                "#unsignedInt",
            )
        )

    @staticmethod
    def _is_float_dt(dt: str) -> bool:
        return dt.endswith(("#double", "#float", "#decimal"))

    @staticmethod
    def _is_bool_dt(dt: str) -> bool:
        return dt.endswith("#boolean")

