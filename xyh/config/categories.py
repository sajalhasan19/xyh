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
  add_categories_bjets(config)
  add_combo_categories(config)

@call_once_on_config()
def add_incl_cat(config: od.Config) -> None:
  cat_incl = config.add_category(  # noqa
    name="cat_incl",
    id=1,
    selection="catid_incl",
    label="Incl",
  )


@call_once_on_config()
def add_lepton_categories(config: od.Config) -> None:

  cat_1e = config.add_category(  # noqa
    name="1e",
    id=10,
    selection="catid_1e",
    label="1 e",
  )

  cat_1mu = config.add_category(  # noqa
    name="1mu",
    id=20,
    selection="catid_1mu",
    label="1 mu",
  )

  cat_1lep = config.add_category(  # noqa
    name="1lep",
    id=30,
    selection="catid_1lep",
    label="1 lep",
  )


@call_once_on_config()
def add_categories_njets(config: od.Config) -> None:
  """
  Adds categories to a *config*, that are typically produced in `ProduceColumns`.
  """

  cat_4j = config.add_category(  # noqa
    name="4jets",
    id=300,
    selection="catid_4jets",
    label="4j",
  )

  cat_ge4j = config.add_category(  # noqa
    name="ge4jets",
    id=400,
    selection="catid_ge4jets",
    label=">=4j",
  )

  cat_5j = config.add_category(  # noqa
    name="5jets",
    id=100,
    selection="catid_5jets",
    label="5j",
  )

  cat_g6j = config.add_category(  # noqa
    name="g6jets",
    id=700,
    selection="catid_g6jets",
    label=">6j",
  )

  cat_ge5j = config.add_category(  # noqa
    name="ge5jets",
    id=500,
    selection="catid_ge5jets",
    label=">=5j",
  )

  cat_6j = config.add_category(  # noqa
    name="6jets",
    id=200,
    selection="catid_6jets",
    label="6j",
  )

  cat_ge6j = config.add_category(  # noqa
    name="ge6jets",
    id=600,
    selection="catid_ge6jets",
    label=">=6j",
  )


@call_once_on_config()
def add_categories_bjets(config: od.Config) -> None:
  cat_0bj = config.add_category(
    name="0bjet",
    id=1000,
    selection="catid_0bjet",
    label="0 b-jets",
  )

  cat_1bj = config.add_category(
    name="1bjet",
    id=2000,
    selection="catid_1bjet",
    label="1 b-jet",
  )

  cat_2bj = config.add_category(
    name="2bjets",
    id=3000,
    selection="catid_2bjets",
    label="2 b-jets",
  )

  cat_3bj = config.add_category(
    name="3bjets",
    id=4000,
    selection="catid_3bjets",
    label="3 b-jets",
  )

  cat_4bj = config.add_category(
    name="4bjets",
    id=5000,
    selection="catid_4bjets",
    label="4 b-jets",
  )

  cat_5bj = config.add_category(
    name="5bjets",
    id=6000,
    selection="catid_5bjets",
    label="5 b-jets",
  )

  cat_g5bj = config.add_category(  # noqa
    name="g5bjets",
    id=12000,
    selection="catid_g5bjets",
    label=">5bj",
  )

  cat_ge1bj = config.add_category(
    name="ge1bjet",
    id=7000,
    selection="catid_ge1bjet",
    label=">=1 b-jet",
  )

  cat_ge2bj = config.add_category(
    name="ge2bjets",
    id=8000,
    selection="catid_ge2bjets",
    label=">=2 b-jets",
  )

  cat_ge3bj = config.add_category(
    name="ge3bjets",
    id=9000,
    selection="catid_ge3bjets",
    label=">=3 b-jets",
  )

  cat_ge4bj = config.add_category(
    name="ge4bjets",
    id=10000,
    selection="catid_ge4bjets",
    label=">=4 b-jets",
  )

  cat_ge5bj = config.add_category(
    name="ge5bjets",
    id=11000,
    selection="catid_ge5bjets",
    label=">=5 b-jets",
  )


@call_once_on_config()
def add_combo_categories(config: od.Config) -> None:
  category_groups = {
    "lepton": [
      config.get_category(name)
      for name in ["1e", "1mu", "1lep"]
    ],
    "bjets": [
      config.get_category(name)
      for name in [
        "0bjet",
        "1bjet",
        "2bjets",
        "3bjets",
        "4bjets",
        "5bjets",
        "g5bjets",
        "ge1bjet",
        "ge2bjets",
        "ge3bjets",
        "ge4bjets",
        "ge5bjets",
      ]
    ],
    "jets": [
      config.get_category(name)
      for name in ["4jets", "5jets", "6jets", "ge4jets", "ge5jets", "ge6jets", "g6jets"]
    ],
  }

  create_category_combinations(config, category_groups, name_fn, kwargs_fn)
