# coding: utf-8

"""
Selection modules for XY(bb)H(tt) lepton selections.
"""

from typing import Tuple
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column
from columnflow.selection import Selector, SelectionResult, selector
from xyh.util import masked_sorted_indices

ak = maybe_import("awkward")


@selector(
  uses={
    "Electron.{pt,eta,phi,charge}",
    "Muon.{pt,eta,phi,charge}",
    "Electron.{mvaIso_WP80,mvaIso_WP90}",
    "Muon.{mediumId,looseId,highPtId,tkIsoId}",
  },
  produces={
    "cutflow.n_mu", "cutflow.n_ele"
  },
  exposed=True,
)
def lepton_selection(
  self: Selector,
  events: ak.Array,
  **kwargs,
) -> Tuple[ak.Array, SelectionResult]:
  
  mu_mask = (
    # Align muon pT with available trigger thresholds
    (events.Muon.pt > 30) &
    (abs(events.Muon.eta) < 2.4) &
    # TODO: High pT ID or midID?
    # TODO: pNet ID?
    (events.Muon.highPtId == 2) &
    (events.Muon.tkIsoId == 2)
  )

  ele_mask = (
    # Keep electron selection symmetric with muons and trigger turn-on
    (events.Electron.pt > 30) &
    (abs(events.Electron.eta) < 2.4) &
    (events.Electron.mvaIso_WP80)
  )

  # Loose lepton definitions used for extra-lepton veto.
  mu_loose_mask = mu_mask | (
    (events.Muon.pt > 15) &
    (abs(events.Muon.eta) < 2.4) &
    (events.Muon.looseId) &
    (events.Muon.tkIsoId >= 1)
  )

  ele_loose_mask = ele_mask | (
    (events.Electron.pt > 15) &
    (abs(events.Electron.eta) < 2.4) &
    (events.Electron.mvaIso_WP90)
  )

  events = set_ak_column(events, "cutflow.n_mu", ak.sum(mu_mask, axis=1))
  events = set_ak_column(events, "cutflow.n_ele", ak.sum(ele_mask, axis=1))

  tight_single_lep = (
    ((events.cutflow.n_mu == 1) & (events.cutflow.n_ele == 0)) |
    ((events.cutflow.n_mu == 0) & (events.cutflow.n_ele == 1))
  )
  loose_single_lep = (ak.sum(mu_loose_mask, axis=1) + ak.sum(ele_loose_mask, axis=1)) == 1
  lep_sel = tight_single_lep & loose_single_lep

  mu_indices = masked_sorted_indices(mu_mask, events.Muon.pt)
  ele_indices = masked_sorted_indices(ele_mask, events.Electron.pt)

  mu_mask = ak.fill_none(mu_mask, False)
  ele_mask = ak.fill_none(ele_mask, False)
  mu_loose_mask = ak.fill_none(mu_loose_mask, False)
  ele_loose_mask = ak.fill_none(ele_loose_mask, False)

  lep_sel = ak.fill_none(lep_sel, False)

  mu = events.Muon[mu_indices]
  ele = events.Electron[ele_indices]

  return events, SelectionResult(
      steps={
        "Lepton": lep_sel,
      },
      objects={
        "Electron": {
          "Electron": ele_indices,
        },
        "Muon": {
          "Muon": mu_indices,
        },
      },
      aux={
        "ele_mask": ele_mask,
        "mu_mask": mu_mask,
        "ele_loose_mask": ele_loose_mask,
        "mu_loose_mask": mu_loose_mask,
      }
    )
