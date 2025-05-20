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
    "Electron.pt", "Electron.eta", "Electron.charge",
    "Muon.pt", "Muon.eta", "Muon.charge",
    "Electron.mvaIso_WP80", "Electron.mvaIso_WP90",
    "Muon.mediumId", "Muon.looseId", "Muon.highPtId", "Muon.tkIsoId"
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
    (events.Muon.pt > 20) &
    (abs(events.Muon.eta) < 2.4) &
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

  lep_sel = ((events.cutflow.n_mu == 2) & (events.cutflow.n_ele == 2))

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
