from pyjelly import options


def register_extension_to_kgtk(extension: str = ".jelly") -> None:
    pass


def _side_effects() -> None:
    register_extension_to_kgtk()


if options.INTEGRATION_SIDE_EFFECTS:
    _side_effects()
