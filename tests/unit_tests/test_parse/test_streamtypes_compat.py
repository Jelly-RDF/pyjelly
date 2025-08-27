from __future__ import annotations

from pyjelly import jelly
from pyjelly.parse.decode import Adapter, Decoder, options_from_frame


def _frame_with_options(
    *, version: int, set_nd: bool | None
) -> tuple[jelly.RdfStreamFrame, jelly.RdfStreamOptions]:
    frame = jelly.RdfStreamFrame()

    opts = jelly.RdfStreamOptions()
    opts.version = version
    opts.stream_name = "t"
    opts.physical_type = jelly.PHYSICAL_STREAM_TYPE_TRIPLES
    opts.logical_type = jelly.LOGICAL_STREAM_TYPE_FLAT_TRIPLES
    opts.max_name_table_size = 128
    opts.max_prefix_table_size = 32
    opts.max_datatype_table_size = 32

    if set_nd is not None and hasattr(opts, "namespace_declarations"):
        opts.namespace_declarations = set_nd

    row = frame.rows.add()
    row.options.CopyFrom(opts)
    return frame, opts


class _DummyAdapter(Adapter):
    def iri(self, iri: str) -> str:
        return iri

    def default_graph(self) -> None:
        return None

    def bnode(self, bnode: str) -> str:
        return bnode

    def literal(
        self, lex: str, language: str | None = None, datatype: str | None = None
    ) -> tuple[str, str | None, str | None]:
        return (lex, language, datatype)


def test_streamtypes_compat_with_v2_and_inferred_nd() -> None:
    frame, raw_opts = _frame_with_options(version=2, set_nd=None)
    parser_opts = options_from_frame(frame, delimited=True)

    dec = Decoder(_DummyAdapter(parser_opts))
    dec.validate_stream_options(raw_opts)


def test_streamtypes_compat_with_v1_and_no_nd() -> None:
    frame, raw_opts = _frame_with_options(version=1, set_nd=None)
    parser_opts = options_from_frame(frame, delimited=True)

    dec = Decoder(_DummyAdapter(parser_opts))
    dec.validate_stream_options(raw_opts)
