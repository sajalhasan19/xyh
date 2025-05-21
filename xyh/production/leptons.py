# coding: utf-8

"""
Column producers related to leptons.
"""
from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

ak = maybe_import("awkward")
np = maybe_import("numpy")
coffea = maybe_import("coffea")
maybe_import("coffea.nanoevents.methods.nanoaod")


@producer(
  uses={
    "category_ids",
    "Electron.{pt,eta,phi,mass,pdgId}",
    "Muon.{pt,eta,phi,mass,pdgId}"
  },
  produces={
    "Leptons.{pt,eta,phi,mass,pdgId}"
  },
)
def leading_lepton(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
  """
  Choose either muon or electron as the main lepton per event
  based on `channel_id` information and write it to a new column
  `Lepton`.
  """

  # extract only LV columns, take leading Ele and Mu in collection
  muon = events.Muon[["pt", "eta", "phi", "mass", "pdgId"]]
  electron = events.Electron[["pt", "eta", "phi", "mass", "pdgId"]]
  leptons = ak.concatenate([
    ak.mask(muon, (ak.pad_none(events.category_ids, 2) == 20)[:, 0]),
    ak.mask(electron, (ak.pad_none(events.category_ids, 2) == 10)[:, 0]),
  ], axis=1)

  # attach lorentz vector behavior to lepton
  leptons = ak.with_name(leptons, "PtEtaPhiMLorentzVector")
  # commit lepton to events array
  events = set_ak_column(events, "Leptons", leptons)

  return events