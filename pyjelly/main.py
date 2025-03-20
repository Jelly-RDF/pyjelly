from __future__ import annotations


def register_mimetypes() -> None:
    import mimetypes

    mimetypes.add_type("application/x-jelly-rdf", ".proto")
