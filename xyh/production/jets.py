# coding: utf-8

"""
Column production methods related to higher-level features.
"""

from __future__ import annotations

from functools import partial

from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

ak = maybe_import("awkward")
np = maybe_import("numpy")

# helper functions
set_ak_bool = partial(set_ak_column, value_type=np.bool_)
set_ak_f32 = partial(set_ak_column, value_type=np.float32)

@producer(
    jet_collection="Jet",
)
def jetId_v12(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
  """
  Producer that extracts correct jet ids for Nano v12
  https://twiki.cern.ch/twiki/bin/view/CMS/JetID13p6TeV?rev=21
  NOTE: this receipe seems to be looser that the "correct" JetId receipe (at least in NanoV13).
  therefore, this should only be used where really necessary (Nano V12).
  In Nano V13 and forward, we can use the columnflow.production.cms.jet.jet_id Producer, which
  recalculates the jetId from scratch using a centrally provided json file.
  """
  jets = events[self.jet_collection]
  abseta = abs(jets.eta)

  # baseline mask (abseta < 2.7)
  passJetId_Tight = (jets.jetId & 2 == 2)

  passJetId_Tight = ak.where(
    (abseta > 2.7) & (abseta <= 3.0),
    passJetId_Tight & (jets.neHEF < 0.99),
    passJetId_Tight,
  )
  passJetId_Tight = ak.where(
    abseta > 3.0,
    passJetId_Tight & (jets.neEmEF < 0.4),
    passJetId_Tight,
  )

  passJetId_TightLepVeto = ak.where(
    abseta <= 2.7,
    passJetId_Tight & (jets.muEF < 0.8) & (jets.chEmEF < 0.8),
    passJetId_Tight,
  )

  events = set_ak_bool(events, "Jet.TightId", passJetId_Tight)
  events = set_ak_bool(events, "Jet.TightLepVeto", passJetId_TightLepVeto)

  return events


@jetId_v12.init
def jetId_v12_init(self: Producer) -> None:
  config_inst = getattr(self, "config_inst", None)

  if config_inst and config_inst.campaign.x.version != 12:
    raise NotImplementedError("jetId_v12 Producer only recommended for Nano v12")


@jetId_v12.post_init
def jetId_v12_post_init(self: Producer, **kwargs) -> None:
  self.uses = {f"{self.jet_collection}.{{pt,eta,phi,mass,jetId,neHEF,neEmEF,muEF,chEmEF}}"}
  self.produces = {f"{self.jet_collection}.{{TightId,TightLepVeto}}"}
