# coding: utf-8

"""
Column production methods related to higher-level features.
"""


from columnflow.production import Producer, producer
from columnflow.production.categories import category_ids
from columnflow.production.normalization import normalization_weights
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

from xyh.production.leptons import leading_lepton
from xyh.production.prepare_objects import prepare_objects
# TODO: Add weight producer, i.e. SFs and all

ak = maybe_import("awkward")
coffea = maybe_import("coffea")
np = maybe_import("numpy")
maybe_import("coffea.nanoevents.methods.nanoaod")


@producer(
  uses={
    category_ids, normalization_weights,
    prepare_objects, leading_lepton,
    "Jet.{pt,eta,phi,mass,rawFactor,btagDeepFlavB}",
    "Bjet.{pt,eta,phi}",
    "MET.{pt,phi}",
    "process_id"
  },
  produces={
    category_ids, normalization_weights,
    prepare_objects, leading_lepton,
    "event_number", "process_id"
  },
)
def default(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
  # Build categories
  events = self[category_ids](events, **kwargs)

  events = self[leading_lepton](events, **kwargs)
  events = self[prepare_objects](events, **kwargs)

  events = set_ak_column(events, "event_number", events.event)

  return events

# @default.init
# def default_init(self: Producer) -> None:
#   # add_categories_bjets(self.config_inst)
#   add_categories_njets(self.config_inst)
