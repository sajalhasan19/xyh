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
def add_all_categories(config: od.Config) -> None:
  add_incl_cat(config)
  add_lepton_categories(config)
  add_categories_njets(config)


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
    selection="catid_1e",
    label="1 Electron",
  )

  cat_1mu = config.add_category(  # noqa
    name="1mu",
    id=20,
    selection="catid_1mu",
    label="1 Muon",
  )


@call_once_on_config()
def add_categories_njets(config: od.Config) -> None:
  """
  Adds categories to a *config*, that are typically produced in `ProduceColumns`.
  """

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
    "jets": [
      config.get_category(name)
      for name in ["5jets", "6jets","4jets"]
    ],
  }

  create_category_combinations(config, category_groups, name_fn, kwargs_fn)