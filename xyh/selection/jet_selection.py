# coding: utf-8

"""
Selection modules for XY(bb)H(tt) jet selections.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Tuple

import law
import order as od

from columnflow.util import maybe_import, DotDict
from columnflow.columnar_util import set_ak_column, optional_column as optional
from columnflow.selection import Selector, SelectionResult, selector
from columnflow.production.cms.jet import jet_id, fatjet_id

from xyh.util import masked_sorted_indices, call_once_on_config, IF_NANO_V12, IF_NANO_geV13
from xyh.production.jets import jetId_v12 # , fatjetId_v12

np = maybe_import("numpy")
ak = maybe_import("awkward")

logger = law.logger.get_logger(__name__)


@selector(
    uses={
        IF_NANO_V12(jetId_v12),
        IF_NANO_geV13(jet_id),
        "Jet.{pt,eta,phi,mass,jetId}", optional("Jet.puId"),
    },
    exposed=True,
)
def jet_selection(
    self: Selector,
    events: ak.Array,
    lepton_results: SelectionResult,
    stats: defaultdict,
    **kwargs,
) -> Tuple[ak.Array, SelectionResult]:
  steps = DotDict()

  # assign local index to all Jets
  events = set_ak_column(events, "local_index", ak.local_index(events.Jet))

  # get correct jet Ids (Jet.TightId and Jet.TightLepVeto)
  if self.has_dep(jetId_v12):
      events = self[jetId_v12](events, **kwargs)
      tight_lep_veto = events.Jet.TightLepVeto
  elif self.has_dep(jet_id):
      events = self[jet_id](events, **kwargs)
      tight_lep_veto = events.Jet.jetId & 6 == 6
  else:
      logger.warning("No Producer found to fix the Jet.jetId, using default Jet.jetId")
      tight_lep_veto = events.Jet.jetId & 6 == 6

  # default jet definition
  jet_mask_loose = (
      (events.Jet.pt >= self.jet_pt) &
      (abs(events.Jet.eta) <= 2.4) &
      (tight_lep_veto)
  )

  # Jet lepton cleaning
  electron = events.Electron[lepton_results.objects.Electron.Electron]
  muon = events.Muon[lepton_results.objects.Muon.Muon]

  jet_mask_incl = (
    (events.Jet.pt >= self.jet_pt) &
    (abs(events.Jet.eta) <= 4.7) &
    tight_lep_veto &
    ak.all(events.Jet.metric_table(electron) > 0.4, axis=2) &
    ak.all(events.Jet.metric_table(muon) > 0.4, axis=2)
  )
  jet_mask = jet_mask_incl & (abs(events.Jet.eta) <= 2.4)
  forward_jet_mask = jet_mask_incl & (abs(events.Jet.eta) > 2.4) & (abs(events.Jet.eta) <= 4.7)

  # apply loose Jet puId to jets with pt below 50 GeV
  # (not in Run3 samples so skip this for now)
  if self.config_inst.x.run == 2:
    jet_pu_mask = (events.Jet.puId >= 4) | (events.Jet.pt > 50)
    jet_mask = jet_mask & jet_pu_mask

  # get the jet indices for pt-sorting of jets
  forward_jet_indices = masked_sorted_indices(forward_jet_mask, events.Jet.pt)
  jet_indices = masked_sorted_indices(jet_mask, events.Jet.pt)

  # add jet steps
  events = set_ak_column(events, "cutflow.n_jet", ak.sum(jet_mask, axis=1))
  steps["nJet1"] = events.cutflow.n_jet >= 1
  steps["nJet2"] = events.cutflow.n_jet >= 2
  steps["nJet3"] = events.cutflow.n_jet >= 3
  steps["nJet4"] = events.cutflow.n_jet >= 4
  if self.config_inst.x("n_jet", 0) > 4:
      steps[f"nJet{self.config_inst.x.n_jet}"] = events.cutflow.n_jet >= self.config_inst.x.n_jet

  # define btag mask
  btag_column = self.config_inst.x.btag_column
  b_score = events.Jet[btag_column]
  # sometimes, jet b-score is nan, so fill it with 0
  if ak.any(np.isnan(b_score)):
    b_score = ak.fill_none(ak.nan_to_none(b_score), 0)
  btag_mask = (jet_mask) & (b_score >= self.config_inst.x.btag_wp_score)

  # add btag steps
  events = set_ak_column(events, "cutflow.n_btag", ak.sum(btag_mask, axis=1))
  steps["nBjet1"] = events.cutflow.n_btag >= 1
  steps["nBjet2"] = events.cutflow.n_btag >= 2
  if self.config_inst.x("n_btag", 0) > 2:
    steps[f"nBjet{self.config_inst.x.n_btag}"] = events.cutflow.n_btag >= self.config_inst.x.n_btag

  # define b-jets as the two b-score leading jets, b-score sorted
  bjet_indices = masked_sorted_indices(jet_mask, b_score)[:, :2]

  # define lightjets as all non b-jets, pt-sorted
  b_idx = ak.fill_none(ak.pad_none(bjet_indices, 2), -1)
  lightjet_indices = jet_indices[(jet_indices != b_idx[:, 0]) & (jet_indices != b_idx[:, 1])]

  # build and return selection results plus new columns
  return events, SelectionResult(
    steps=steps,
    objects={
      "Jet": {
        "Jet": jet_indices,
        "ForwardJet": forward_jet_indices,
        "Bjet": bjet_indices,
        "Lightjet": lightjet_indices,
      },
    },
    aux={
      "jet_mask": jet_mask,
      "n_central_jets": ak.num(jet_indices),
      "ht": ak.sum(events.Jet.pt[jet_mask], axis=1),
    },
  )

@jet_selection.init
def jet_selection_init(self: Selector) -> None:
  # configuration of defaults
  self.jet_pt = self.config_inst.x("jet_pt", 25)

  # Add shift dependencies
  self.shifts |= {
    shift_inst.name
    for shift_inst in self.config_inst.shifts
    if shift_inst.has_tag(("jec", "jer"))
  }

  # add btag requirement
  self.uses.add(f"Jet.{self.config_inst.x.btag_column}")

  # update selector steps labels
  self.config_inst.x.selector_step_labels = self.config_inst.x("selector_step_labels", {})
  self.config_inst.x.selector_step_labels.update({
    "nJet1": r"$N_{jets}^{AK4} \geq 1$",
    "nJet2": r"$N_{jets}^{AK4} \geq 2$",
    "nJet3": r"$N_{jets}^{AK4} \geq 3$",
    "nJet4": r"$N_{jets}^{AK4} \geq 4$",
    "nBjet1": r"$N_{jets}^{BTag} \geq 1$",
    "nBjet2": r"$N_{jets}^{BTag} \geq 2$",
  })

  if self.config_inst.x("do_cutflow_features", False):
    # add cutflow features to *produces* only when requested
    self.produces.add("cutflow.n_jet")
    self.produces.add("cutflow.n_btag")

    @call_once_on_config
    def add_jet_cutflow_variables(config: od.Config):
      config.add_variable(
        name="cf_n_jet",
        expression="cutflow.n_jet",
        binning=(7, -0.5, 6.5),
        x_title="Number of jets",
        discrete_x=True,
      )
      config.add_variable(
        name="cf_n_btag",
        expression="cutflow.n_btag",
        binning=(7, -0.5, 6.5),
        x_title=f"Number of b-tagged jets ({self.config_inst.x.b_tagger}, {self.config_inst.x.btag_wp} WP)",
        discrete_x=True,
      )

    add_jet_cutflow_variables(self.config_inst)

