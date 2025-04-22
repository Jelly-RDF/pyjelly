from pyjelly import jelly
from pyjelly.options import StreamOptions


def options_from_frame(
    frame: jelly.RdfStreamFrame, *, delimited: bool
) -> StreamOptions:
    row = frame.rows[0]
    return StreamOptions(
        stream_name=row.options.stream_name,
        version=row.options.version,
        name_lookup_size=row.options.max_name_table_size,
        prefix_lookup_size=row.options.max_prefix_table_size,
        datatype_lookup_size=row.options.max_datatype_table_size,
        delimited=delimited,
    )
