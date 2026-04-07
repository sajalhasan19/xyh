# coding: utf-8

"""
XYH Categorization methods.
"""

from columnflow.categorization import Categorizer, categorizer
from columnflow.util import maybe_import

import order as od

ak = maybe_import("awkward")
np = maybe_import("numpy")

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

@categorizer(uses={"event"}, call_force=True)
def catid_1lep(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  n_electrons = ak.num(events.Electron, axis=-1)
  n_muons = ak.num(events.Muon, axis=-1)
  mask = (n_electrons + n_muons) == 1
  return events, mask

@categorizer(uses={"Jet"}, call_force=True)
def catid_2jets(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  n_jets = ak.num(events.Jet, axis=-1)
  mask = (n_jets == 2)
  return events, mask

@categorizer(uses={"Jet"}, call_force=True)
def catid_ge2jets(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  n_jets = ak.num(events.Jet, axis=-1)
  mask = (n_jets >= 2)
  return events, mask

@categorizer(uses={"Jet"}, call_force=True)
def catid_5jets(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  n_jets = ak.num(events.Jet, axis=-1)
  mask = (n_jets == 5)
  return events, mask


@categorizer(uses={"Jet"}, call_force=True)
def catid_4jets(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  n_jets = ak.num(events.Jet, axis=-1)
  mask = (n_jets == 4)
  return events, mask


@categorizer(uses={"Jet"}, call_force=True)
def catid_ge4jets(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  n_jets = ak.num(events.Jet, axis=-1)
  mask = (n_jets >= 4)
  return events, mask


@categorizer(uses={"Jet"}, call_force=True)
def catid_6jets(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  n_jets = ak.num(events.Jet, axis=-1)
  mask = (n_jets == 6)
  return events, mask


@categorizer(uses={"Jet"}, call_force=True)
def catid_ge5jets(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  n_jets = ak.num(events.Jet, axis=-1)
  mask = (n_jets >= 5)
  return events, mask

@categorizer(uses={"Jet"}, call_force=True)
def catid_g6jets(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  n_jets = ak.num(events.Jet, axis=-1)
  mask = (n_jets > 6)
  return events, mask

@categorizer(uses={"Jet"}, call_force=True)
def catid_ge6jets(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  n_jets = ak.num(events.Jet, axis=-1)
  mask = (n_jets >= 6)
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
  n_bjets = ak.num(events.Bjet, axis=-1)
  mask = (n_bjets == 2)
  return events, mask

@categorizer(uses={"Bjet"}, call_force=True)
def catid_3bjets(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  n_bjets = ak.num(events.Bjet, axis=-1)
  mask = (n_bjets == 3)
  return events, mask

@categorizer(uses={"Bjet"}, call_force=True)
def catid_4bjets(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  n_bjets = ak.num(events.Bjet, axis=-1)
  mask = (n_bjets == 4)
  return events, mask

@categorizer(uses={"Bjet"}, call_force=True)
def catid_5bjets(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  n_bjets = ak.num(events.Bjet, axis=-1)
  mask = (n_bjets == 5)
  return events, mask

@categorizer(uses={"Bjet"}, call_force=True)
def catid_g5bjets(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  n_bjets = ak.num(events.Bjet, axis=-1)
  mask = (n_bjets > 5)
  return events, mask

@categorizer(uses={"Bjet"}, call_force=True)
def catid_ge1bjet(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  n_bjets = ak.num(events.Bjet, axis=-1)
  mask = (n_bjets >= 1)
  return events, mask


@categorizer(uses={"Bjet"}, call_force=True)
def catid_ge2bjets(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  n_bjets = ak.num(events.Bjet, axis=-1)
  mask = (n_bjets >= 2)
  return events, mask


@categorizer(uses={"Bjet"}, call_force=True)
def catid_ge3bjets(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  n_bjets = ak.num(events.Bjet, axis=-1)
  mask = (n_bjets >= 3)
  return events, mask


@categorizer(uses={"Bjet"}, call_force=True)
def catid_ge4bjets(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  n_bjets = ak.num(events.Bjet, axis=-1)
  mask = (n_bjets >= 4)
  return events, mask


@categorizer(uses={"Bjet"}, call_force=True)
def catid_ge5bjets(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
  n_bjets = ak.num(events.Bjet, axis=-1)
  mask = (n_bjets >= 5)
  return events, mask


@categorizer(uses={"Jet"}, call_force=True)
def catid_dr_jj(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:

  has2j = ak.num(events.Jet) >= 2

  dR_jj = ak.where(
      has2j,
      events.Jet[:, 0].delta_r(events.Jet[:, 1]),
      np.nan,
  )

  mask = has2j & (dR_jj <= 1.4)

  return events, mask


@categorizer(uses={"Lightjet"}, call_force=True)
def catid_dr_qq(self: Categorizer, events: ak.Array, **kwargs):
    has2q = ak.num(events.Lightjet) >= 2
    dphi = ((events.Lightjet[:,0].phi - events.Lightjet[:,1].phi + np.pi) % (2*np.pi)) - np.pi
    deta = events.Lightjet[:,0].eta - events.Lightjet[:,1].eta
    dR_qq = ak.where(has2q, np.sqrt(deta*deta + dphi*dphi), np.nan)
    mask = has2q & (dR_qq <= 1.4)
    return events, mask

# @categorizer(uses={"Bjet"}, call_force=True)
# def catid_dr_bb(self: Categorizer, events: ak.Array, **kwargs):
#     has2b = ak.num(events.Bjet) >= 2
#     dphi = ((events.Bjet[:,0].phi - events.Bjet[:,1].phi + np.pi) % (2*np.pi)) - np.pi
#     deta = events.Bjet[:,0].eta - events.Bjet[:,1].eta
#     dR_bb = ak.where(has2b, np.sqrt(deta*deta + dphi*dphi), np.nan)
#     mask = has2b & (dR_bb <= 1.4)
#     return events, mask


# @categorizer(uses={"Bjet"}, call_force=True)
# def catid_dr_qq(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:

#   has2b = ak.num(events.Bjet) >= 2

#   dR_bb = ak.where(
#       has2b,
#       events.Bjet[:, 0].delta_r(events.Bjet[:, 1]),
#       np.nan,
#   )

#   print("ΔR range (bb):", ak.min(dR_bb), ak.max(dR_bb))

#   mask = has2b & (dR_bb <= 1.4)

#   return events, mask  





# @categorizer(uses={"Bjet"}, call_force=True)
# def catid_dr_bb(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:

#   bjet_pairs = ak.combinations(events.Bjet, 2, fields=["b1", "b2"])
#   dR_bb = bjet_pairs.b1.delta_r(bjet_pairs.b2)

#   # debug: print min/max of ΔR
#   print("ΔR range (bb):", ak.min(dR_bb), ak.max(dR_bb))

#   mask = ak.any((dR_bb >= 0.4) & (dR_bb <=1.4), axis=1)

#   return events, mask


# @categorizer(uses={"Lightjet"}, call_force=True)
# def catid_dr_qq(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:

#   lightjet_pairs = ak.combinations(events.Lightjet, 2, fields=["q1", "q2"])
#   dR_qq = lightjet_pairs.q1.delta_r(lightjet_pairs.q2)

#   # debug: print min/max of ΔR
#   print("ΔR range (qq):", ak.min(dR_qq), ak.max(dR_qq))

#   mask = ak.any((dR_qq >= 0.4) & (dR_qq <=1.4), axis=1)

#   return events, mask
