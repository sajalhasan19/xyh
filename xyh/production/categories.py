# coding: utf-8

"""
Selection methods defining categories based on selection step results.
"""

from __future__ import annotations

from columnflow.util import maybe_import
from columnflow.categorization import Categorizer, categorizer

np = maybe_import("numpy")
ak = maybe_import("awkward")


@categorizer(uses={"BJet"}, call_force=True)
def catid_2bjets(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
    mask = (ak.num(events.BJet, axis=-1) >= 2)
    return events, mask


@categorizer(uses={"BJet"}, call_force=True)
def catid_1bjets(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
    mask = (ak.num(events.BJet, axis=-1) == 1)
    return events, mask


@categorizer(uses={"BJet"}, call_force=True)
def catid_0bjets(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
    mask = (ak.num(events.BJet, axis=-1) == 0)
    return events, mask


@categorizer(uses={"Jet"}, call_force=True)
def catid_6jets(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
    mask = (ak.num(events.Jet, axis=-1) >= 6)
    return events, mask


@categorizer(uses={"Jet"}, call_force=True)
def catid_5jets(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
    mask = (ak.num(events.Jet, axis=-1) == 5)
    return events, mask

@categorizer(uses={"Jet"}, call_force=True)
def catid_4jets(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
    mask = (ak.num(events.Jet, axis=-1) == 4)
    return events, mask