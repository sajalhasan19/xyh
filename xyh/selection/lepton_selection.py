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
    (events.Muon.pt > 10) &
    (abs(events.Muon.eta) < 2.4) &
    # TODO: High pT ID or midID?
    # TODO: pNet ID?
    (events.Muon.highPtId == 2) &
    (events.Muon.tkIsoId == 2)
  )

  ele_mask = (
    (events.Electron.pt > 20) &
    (abs(events.Electron.eta) < 2.4) &
    (events.Electron.mvaIso_WP80)
  )

  events = set_ak_column(events, "cutflow.n_mu", ak.sum(mu_mask, axis=1))
  events = set_ak_column(events, "cutflow.n_ele", ak.sum(ele_mask, axis=1))

  # TODO: Maybe veto additional loose leptons
  # See AZH as a reference
  lep_sel = (
    ((events.cutflow.n_mu == 1) & (events.cutflow.n_ele == 0)) |
    ((events.cutflow.n_mu == 0) & (events.cutflow.n_ele == 1))
  )

  mu_indices = masked_sorted_indices(mu_mask, events.Muon.pt)
  ele_indices = masked_sorted_indices(ele_mask, events.Electron.pt)

  mu_mask = ak.fill_none(mu_mask, False)
  ele_mask = ak.fill_none(ele_mask, False)

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
      }
    )
