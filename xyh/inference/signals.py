# coding: utf-8

"""
Helper utilities describing the list of available XYH signal hypotheses.

When running training or evaluation helpers we often need the complete set of
signal process names (``XYH_SIGNAL_PROCESSES``) and the corresponding dataset
names (``XYH_SIGNAL_DATASETS``).  The information is defined in
``modules/cmsdb/cmsdb/processes/xyh.py``; this module parses that source file so
we do not have to duplicate a long manual list here.
"""

from __future__ import annotations

from collections import OrderedDict
from functools import cache
from pathlib import Path
import re


_XYH_PROCESS_PATTERN = re.compile(r'name="(xyh_sl_x\d+_y\d+)"')


def _mass_key(name: str) -> tuple[int, int]:
    """
    Split an ``xyh_sl_x<mass>_y<mass>`` string into integer masses, returning a
    tuple that can be used for natural sorting.
    """
    match = re.search(r"x(\d+)_y(\d+)", name)
    if not match:
        return (0, 0)
    return (int(match.group(1)), int(match.group(2)))


@cache
def _collect_signal_process_names() -> tuple[str, ...]:
    """
    Scan the cmsdb process definition for XYH entries and return the unique
    process names in ascending mass order.
    """
    source_file = Path(__file__).resolve().parents[2] / "modules" / "cmsdb" / "cmsdb" / "processes" / "xyh.py"
    if not source_file.exists():
        raise FileNotFoundError(
            f"Cannot locate XYH process definition at '{source_file}'. "
            "Ensure the cmsdb submodule is available."
        )

    names = OrderedDict()
    text = source_file.read_text(encoding="utf-8")
    for match in _XYH_PROCESS_PATTERN.finditer(text):
        names.setdefault(match.group(1), None)

    return tuple(sorted(names.keys(), key=_mass_key))


@cache
def _collect_signal_dataset_names() -> tuple[str, ...]:
    """
    Dataset names follow the convention ``<process>_madgraph``; build the tuple
    based on the collected process names.
    """
    return tuple(f"{process}_madgraph" for process in _collect_signal_process_names())


# Public constants -----------------------------------------------------------------

XYH_SIGNAL_PROCESSES: tuple[str, ...] = _collect_signal_process_names()
XYH_SIGNAL_DATASETS: tuple[str, ...] = _collect_signal_dataset_names()


__all__ = ["XYH_SIGNAL_PROCESSES", "XYH_SIGNAL_DATASETS"]
