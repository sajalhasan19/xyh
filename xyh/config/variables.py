# coding: utf-8

"""
Definition of variables.
"""

import order as od

from columnflow.util import maybe_import
from columnflow.columnar_util import EMPTY_FLOAT

np = maybe_import("numpy")
ak = maybe_import("awkward")


def add_variables(config: od.Config) -> None:
  """
  Adds all variables to a *config* that are present after `ReduceEvents`
  without calling any producer
  """

  # (the "event", "run" and "lumi" variables are required for some cutflow plotting task,
  # and also correspond to the minimal set of columns that coffea's nano scheme requires)
  config.add_variable(
    name="event",
    expression="event",
    binning=(1, 0.0, 1.0e9),
    x_title="Event number",
    discrete_x=False,
  )
  config.add_variable(
    name="run",
    expression="run",
    binning=(1, 100000.0, 500000.0),
    x_title="Run number",
    discrete_x=True,
  )
  config.add_variable(
    name="lumi",
    expression="luminosityBlock",
    binning=(1, 0.0, 5000.0),
    x_title="Luminosity block",
    discrete_x=True,
  )

  #
  # Weights
  #

  # TODO: implement tags in columnflow; meanwhile leave these variables commented out (as they only work for mc)
  config.add_variable(
    name="npvs",
    expression="PV.npvs",
    binning=(51, -.5, 50.5),
    x_title="Number of primary vertices",
    discrete_x=True,
  )

  #
  # Object properties
  #

  config.add_variable(
    name="jets_pt",
    expression="Jet.pt",
    binning=(40, 0, 400),
    unit="GeV",
    x_title="$p_{T}$ of all jets",
  )

  config.add_variable(
    name="n_jets",
    expression=lambda events: ak.num(events.Jet["pt"], axis=1),
    aux={"inputs": {"Jet.pt"}},
    binning=(12, -0.5, 11.5),
    discrete_x=True,
    x_title="Number of jets",
  )

  config.add_variable(
    name="n_bjets",
    expression=lambda events: ak.num(events.Bjet["pt"], axis=1),
    aux={"inputs": {"Bjet.pt"}},
    binning=(5, -0.5, 4.5),
    discrete_x=True,
    x_title="Number of bjets",
  )

  config.add_variable(
    name="n_leps",
    expression=lambda events: ak.num(events.Leptons["pt"], axis=1),
    aux={"inputs": {"Leptons.pt"}},
    binning=(5, -0.5, 4.5),
    discrete_x=True,
    x_title="Number of leptons",
  )

  config.add_variable(
    name="jets_btag",
    expression="Jet.btagDeepFlavB",
    binning=(20, 0, 1),
    unit="",
    x_title="Btag Score Deep Jet",
  )

  config.add_variable(
    name="category_ids",
    expression="category_ids",
    binning=(20, 0, 100000),
    unit="",
    x_title="Event category",
  )

  # Jets (3 pt-leading jets)
  for i in range(6):
    config.add_variable(
      name=f"jet{i+1}_pt",
      expression=f"Jet.pt[:,{i}]",
      null_value=EMPTY_FLOAT,
      binning=(40, 0., 400.),
      unit="GeV",
      x_title=r"Jet %i $p_{T}$" % (i + 1),
    )
    config.add_variable(
      name=f"jet{i+1}_eta",
      expression=f"Jet.eta[:,{i}]",
      null_value=EMPTY_FLOAT,
      binning=(50, -2.5, 2.5),
      x_title=r"Jet %i $\eta$" % (i + 1),
    )
    config.add_variable(
      name=f"jet{i+1}_phi",
      expression=f"Jet.phi[:,{i}]",
      null_value=EMPTY_FLOAT,
      binning=(40, -3.2, 3.2),
      x_title=r"Jet %i $\phi$" % (i + 1),
    )
    config.add_variable(
      name=f"jet{i+1}_mass",
      expression=f"Jet.mass[:,{i}]",
      null_value=EMPTY_FLOAT,
      binning=(40, 0, 200),
      unit="GeV",
      x_title=r"Jet %i mass" % (i + 1),
    )

  for i in range(2):
    config.add_variable(
      name=f"Lepton{i+1}_pt",
      expression=f"Leptons.pt[:,{i}]",
      null_value=EMPTY_FLOAT,
      binning=(40, 0., 400.),
      unit="GeV",
      x_title=r"Lepton %i $p_{T}$" % (i + 1),
    )
    config.add_variable(
      name=f"Lepton{i+1}_eta",
      expression=f"Leptons.eta[:,{i}]",
      null_value=EMPTY_FLOAT,
      binning=(50, -2.5, 2.5),
      x_title=r"Lepton %i $\eta$" % (i + 1),
    )
    config.add_variable(
      name=f"Lepton{i+1}_phi",
      expression=f"Leptons.phi[:,{i}]",
      null_value=EMPTY_FLOAT,
      binning=(40, -3.2, 3.2),
      x_title=r"Lepton %i $\phi$" % (i + 1),
    )

  config.add_variable(
      name=f"wlnu_mass",
      expression="mlnu",
      null_value=EMPTY_FLOAT,
      binning=(40, 0., 400.),
      unit="GeV",
      x_title=r"$m_{l\nu}$",
  )

  config.add_variable(
      name="wlnu_mt",
      expression="mtlnu",
      null_value=EMPTY_FLOAT,
      binning=(40, 0., 400.),
      unit="GeV",
      x_title=r"$m^{T}_{l\nu}$",
  )

  config.add_variable(
      name="wlnu_pt",
      expression="wboson.pt",
      null_value=EMPTY_FLOAT,
      binning=(40, 0., 400.),
      unit="GeV",
      x_title=r"$p^{T}_{l\nu}$",
  )

  # Cutflow Variables
  # config.add_variable(
  #   name="cf_n_jet",
  #   expression="cutflow.n_jet",
  #   binning=(11, -0.5, 10.5),
  #   x_title=r"Number of jets ($p_{T}$ > 30 GeV, $|\eta| < 2.4$)",
  # )
  # config.add_variable(
  #   name="cf_n_bjet",
  #   expression="cutflow.n_bjet",
  #   binning=(11, -0.5, 10.5),
  #   x_title=r"Number of b-taggeg jets ($p_{T}$ > 30 GeV, $|\eta| < 2.4$)",
  # )
  config.add_variable(
    name="cf_n_ele",
    expression="cutflow.n_ele",
    binning=(5, -0.5, 4.5),
    x_title=r"Number of electrons ($p_{T}$ > 20 GeV, $|\eta| < 2.4$ + tight Iso)",
  )
  config.add_variable(
    name="cf_n_muo",
    expression="cutflow.n_mu",
    binning=(5, -0.5, 4.5),
    x_title=r"Number of muons ($p_{T}$ > 20 GeV, $|\eta| < 2.4$ + tight Id)",
  )

  # TODO: Could add here variables about cutflow in lepton_selection.py
  # See AZH as example
