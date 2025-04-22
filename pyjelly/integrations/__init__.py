from pyjelly import options


def side_effects() -> None:
    try:
        import rdflib
    except ImportError:
        from pyjelly.integrations.with_rdflib import register_extension_to_rdflib

        register_extension_to_rdflib()


if options.INTEGRATION_SIDE_EFFECTS:
    side_effects()
