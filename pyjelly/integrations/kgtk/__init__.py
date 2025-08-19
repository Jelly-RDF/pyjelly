import mimetypes
from pyjelly import options


def register_extension_to_kgtk(extension: str = ".jelly", mimetype: str = "application/x-jelly-kgtk") -> None:
    ext = extension if extension.startswith(".") else f".{extension}"
    mimetypes.add_type(mimetype, ext, strict=False)


def _side_effects() -> None:
    register_extension_to_kgtk()


if options.INTEGRATION_SIDE_EFFECTS:
    _side_effects()
