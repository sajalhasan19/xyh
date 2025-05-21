# coding: utf-8

"""
Selection methods defining categories based on selection step results.
"""

from __future__ import annotations

from columnflow.util import maybe_import
from columnflow.categorization import Categorizer, categorizer

np = maybe_import("numpy")
ak = maybe_import("awkward")


@categorizer(uses={"event"}, call_force=True)
def catid_incl(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  mask = ak.ones_like(events.event) > 0
  return events, mask


@categorizer(uses={"event"}, call_force=True)
def catid_selection_1e(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  mask = (ak.num(events.Electron, axis=-1) == 1) & (ak.num(events.Muon, axis=-1) == 0)
  return events, mask


@categorizer(uses={"event"}, call_force=True)
def catid_selection_1mu(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  mask = (ak.num(events.Electron, axis=-1) == 0) & (ak.num(events.Muon, axis=-1) == 1)
  return events, mask


@categorizer(uses={"Electron.pt", "Muon.pt", "cutflow.*"}, call_force=True)
def catid_1e(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  mask = ((events.cutflow.n_ele == 1) & (events.cutflow.n_mu == 0))
  return events, mask


@categorizer(uses={"Electron.pt", "Muon.pt", "cutflow.*"}, call_force=True)
def catid_1mu(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  mask = ((events.cutflow.n_ele == 0) & (events.cutflow.n_mu == 1))
  return events, mask