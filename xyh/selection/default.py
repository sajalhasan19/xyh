# coding: utf-8

"""
XY(bb)H(tt) selection methods
"""

from operator import and_
from functools import reduce
from collections import defaultdict
from typing import Tuple

from columnflow.util import maybe_import

from columnflow.selection.stats import increment_stats
from columnflow.selection import Selector, SelectionResult, selector
from columnflow.selection.cms.met_filters import met_filters
from columnflow.selection.cms.json_filter import json_filter
from columnflow.selection.cms.jets import jet_veto_map

from columnflow.production.util import attach_coffea_behavior
from columnflow.production.cms.mc_weight import mc_weight
from columnflow.production.categories import category_ids
from columnflow.production.processes import process_ids

# Here import your selection modules from xyh/selection
# from xyh.selection.trigger_selection import trigger_selection
from xyh.selection.lepton_selection import lepton_selection
from xyh.selection.jet_selection import jet_selection

# Define the categories used in the analysis
from xyh.config.categories import add_categories_production


np = maybe_import("numpy")
ak = maybe_import("awkward")


@selector(
  uses={
    process_ids, attach_coffea_behavior,
    mc_weight, category_ids, increment_stats,
    met_filters, json_filter, jet_veto_map,
    lepton_selection, jet_selection, # trigger_selection
  },
  produces={
    process_ids, attach_coffea_behavior,
    mc_weight, category_ids, increment_stats,
    met_filters, json_filter, jet_veto_map,
    lepton_selection, jet_selection, # trigger_selection
  },
  exposed=True,
  check_used_columns=False,
  check_produced_columns=False,
)
def default(
  self: Selector,
  events: ak.Array,
  stats: defaultdict,
  **kwargs
) -> Tuple[ak.Array, SelectionResult]:

  if self.dataset_inst.is_mc:
    events = self[mc_weight](events, **kwargs)

  # prepare the selection results that are updated at every step
  results = SelectionResult()

  # MET filters and jet veto maps
  events, met_filters_results = self[met_filters](events, **kwargs)
  results += met_filters_results

  events, jet_veto_results = self[jet_veto_map](events, **kwargs)
  results += jet_veto_results

  # JSON filter (data-only)
  if self.dataset_inst.is_data:
      events, json_filter_results = self[json_filter](events, **kwargs)
      results += json_filter_results

  # Now analysis specific selections
  events, results_lepton = self[lepton_selection](events, **kwargs) 
  results += results_lepton

  events, results_jet = self[jet_selection](events, **kwargs)
  results += results_jet

  # TODO: Implement trigger selection
  # events, results_trig = self[trigger_selection](events, **kwargs)
  # results += results_trig

  # Post selection, build process IDs and categories
  events = self[process_ids](events, **kwargs)
  events = self[category_ids](events, **kwargs)

  # Combine all the selections together to build full evt mask
  results.event = reduce(and_, results.steps.values())
  results.event = ak.fill_none(results.event, False)

  weight_map = {
    "num_events": Ellipsis,
    "num_events_selected": results.event,
  }
  group_map = {}
  if self.dataset_inst.is_mc:
    weight_map = {
        **weight_map,
        # mc weight for all events
        "sum_mc_weight": (events.mc_weight, Ellipsis),
        "sum_mc_weight_selected": (events.mc_weight, results.event),
    }
    group_map = {
        # per process
        "process": {
            "values": events.process_id,
            "mask_fn": (lambda v: events.process_id == v),
        },
    }
  events, results = self[increment_stats](
      events,
      results,
      stats,
      weight_map=weight_map,
      group_map=group_map,
      **kwargs,
  )

  return events, results

@default.init
def default_init(self: Selector) -> None:
  if not self.config_inst.get_aux("has_categories_sel", False):
    add_categories_production(self.config_inst)
    self.config_inst.x.has_categories_sel = True