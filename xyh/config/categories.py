# coding: utf-8

"""
Definition of categories.

Categories are assigned a unique integer ID according to a fixed numbering
scheme, with digits/groups of digits indicating the different category groups:

TODO, document here.
"""

import law

from columnflow.util import maybe_import
from columnflow.config_util import create_category_combinations

from xyh.util import call_once_on_config

import order as od

logger = law.logger.get_logger(__name__)

np = maybe_import("numpy")
ak = maybe_import("awkward")

def name_fn(categories: dict[str, od.Category]):
  """Naming function for automatically generated combined categories."""
  return "__".join(cat.name for cat in categories.values() if cat)


def kwargs_fn(categories: dict[str, od.Category]):
  """Customization function for automatically generated combined categories."""
  return {
    "id": sum(cat.id for cat in categories.values()),
    "selection": [cat.selection for cat in categories.values()],
    "label": "\n".join(
        cat.label for cat in categories.values()
    ),
  }


def skip_fn(categories: dict[str, od.Category]):
  """Custom function for skipping certain category combinations."""
  return False  # don't skip


@call_once_on_config()
def add_categories_selection(config: od.Config) -> None:
  add_lepton_categories(config)
  add_incl_cat(config)


@call_once_on_config()
def add_incl_cat(config: od.Config) -> None:
  cat_incl = config.add_category(  # noqa
    name="cat_incl",
    id=1,
    selection="catid_incl",
    label="Inclusive",
  )


@call_once_on_config()
def add_lepton_categories(config: od.Config) -> None:

  cat_1e = config.add_category(  # noqa
    name="1e",
    id=10,
    selection="catid_selection_1e",
    label="1 Electron",
  )

  cat_1mu = config.add_category(  # noqa
    name="1mu",
    id=20,
    selection="catid_selection_1mu",
    label="1 Muon",
  )


@call_once_on_config()
def add_categories_production(config: od.Config) -> None:
  """
  Adds categories to a *config*, that are typically produced in `ProduceColumns`.
  """

  cat_1e = config.get_category("1e")
  cat_1e.selection = "catid_1e"

  cat_1mu = config.get_category("1mu")
  cat_1mu.selection = "catid_1mu"


@call_once_on_config()
def add_categories_bjets(config: od.Config) -> None:
  """
  Adds categories to a *config*, that are typically produced in `ProduceColumns`.
  """

  #
  # switch existing categories to different production module
  #
  cat_SR = config.add_category(  # noqa
    name="2bjets",
    id=3000,
    selection="catid_2bjets",
    label=">=2 B-Jets",
  )

  cat_CR= config.add_category(  # noqa
    name="1bjets",
    id=2000,
    selection="catid_1bjets",
    label="1 B-Jets",
  )

  cat_CR= config.add_category(  # noqa
    name="0bjets",
    id=1000,
    selection="catid_0bjets",
    label="0 B-Jets",
  )


@call_once_on_config()
def add_categories_njets(config: od.Config) -> None:
  """
  Adds categories to a *config*, that are typically produced in `ProduceColumns`.
  """

  #
  # switch existing categories to different production module
  #

  cat_SR = config.add_category(  # noqa
    name="4jets",
    id=30000,
    selection="catid_4jets",
    label="4 Jets",
  )

  cat_SR = config.add_category(  # noqa
    name="5jets",
    id=10000,
    selection="catid_5jets",
    label="5 Jets",
  )

  cat_CR= config.add_category(  # noqa
    name="6jets",
    id=20000,
    selection="catid_6jets",
    label="6 or more Jets",
  )

  category_groups = {
    "lepton": [
      config.get_category(name)
      for name in ["1e", "1mu"]
    ],
    "b_jets": [
      config.get_category(name)
      for name in ["2bjets", "1bjets", "0bjets"]
    ],
    "jets": [
      config.get_category(name)
      for name in ["5jets", "6jets","4jets"]
    ],
  }
  create_category_combinations(config, category_groups, name_fn, kwargs_fn)