# coding: utf-8

"""
XYH Categorization methods.
"""

from columnflow.categorization import Categorizer, categorizer
from columnflow.util import maybe_import

ak = maybe_import("awkward")


#
# categorizer functions used by categories definitions
#

@categorizer(uses={"event"})
def catid_incl(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  # fully inclusive selection
  return events, ak.ones_like(events.event) == 1


@categorizer(uses={"event"}, call_force=True)
def catid_1e(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  mask = (ak.num(events.Electron, axis=-1) == 1) & (ak.num(events.Muon, axis=-1) == 0)
  return events, mask


@categorizer(uses={"event"}, call_force=True)
def catid_1mu(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  mask = (ak.num(events.Electron, axis=-1) == 0) & (ak.num(events.Muon, axis=-1) == 1)
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


@categorizer(uses={"Bjet"}, call_force=True)
def catid_0bjet(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  mask = (ak.num(events.Bjet, axis=-1) == 0)
  return events, mask


@categorizer(uses={"Bjet"}, call_force=True)
def catid_1bjet(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  mask = (ak.num(events.Bjet, axis=-1) == 1)
  return events, mask


@categorizer(uses={"Bjet"}, call_force=True)
def catid_2bjets(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  mask = (ak.num(events.Bjet, axis=-1) >= 2)
  return events, mask

