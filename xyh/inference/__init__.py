# coding: utf-8

"""Ensure inference models are imported when the :mod:`xyh.inference` package is loaded."""

# Re-export inference models so the columnflow registration side-effects run on import.
from . import example  # noqa: F401

# ``datacards`` is optional while the dedicated inference model is in flux. Avoid
# import errors when the module is absent (e.g. after clean-up) so the rest of the
# task registry still loads.
try:  # pragma: no cover - defensive guard
    from . import datacards  # noqa: F401
except ImportError:
    datacards = None
