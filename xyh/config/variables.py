# coding: utf-8

"""
Definition of variables.
"""

import order as od

from columnflow.util import maybe_import
from columnflow.columnar_util import EMPTY_FLOAT
from xyh.inference.signals import XYH_SIGNAL_PROCESSES

np = maybe_import("numpy")
ak = maybe_import("awkward")

MODEL_NAMES = [
    f"xyh_binary_{name.removeprefix('xyh_sl_')}"
    for name in XYH_SIGNAL_PROCESSES
]

LOGIT_CLIP_EPS = 1.0e-6


def _logit_transform(values):
  """
  Apply a numerically stable logit transform to NN scores and keep invalid
  entries at the configured null value.
  """
  if np is None or ak is None:
    raise RuntimeError("numpy and awkward are required for logit-based NN score variables")

  valid = (values >= 0.0) & (values <= 1.0)
  safe = ak.where(valid, values, 0.5)
  safe = ak.where(safe < LOGIT_CLIP_EPS, LOGIT_CLIP_EPS, safe)
  safe = ak.where(safe > 1.0 - LOGIT_CLIP_EPS, 1.0 - LOGIT_CLIP_EPS, safe)
  logits = np.log(safe / (1.0 - safe))
  return ak.where(valid, logits, -1.0)


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
    name="met_pt",
    expression="MET.pt",
    binning=(40, 0, 400),
    unit="GeV",
    x_title="MET $p_{T}$",
  )

  config.add_variable(
    name="met_phi",
    expression="MET.phi",
    null_value=EMPTY_FLOAT,
    binning=(50, -2.5, 2.5),
    x_title="MET $eta$",
  )

  config.add_variable(
    name="n_jets",
    expression=lambda events: ak.num(events.Jet["pt"], axis=1),
    aux={"inputs": {"Jet.pt"}, "overflow":True},
    binning=(12, -0.5, 11.5),
    discrete_x=True,
    x_title="Number of jets",
  )

  config.add_variable(
    name="n_bjets",
    expression=lambda events: ak.num(events.Bjet["pt"], axis=1),
    aux={"inputs": {"Bjet.pt"}, "overflow": True},
    binning=(6, -0.5, 6.5),
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

  # B-jets (pt-leading b-jets)
  for i in range(4):
    config.add_variable(
      name=f"bjet{i+1}_pt",
      expression=f"Bjet.pt[:,{i}]",
      null_value=EMPTY_FLOAT,
      binning=(40, 0., 400.),
      unit="GeV",
      x_title=r"B-jet %i $p_{T}$" % (i + 1),
    )
    config.add_variable(
      name=f"bjet{i+1}_eta",
      expression=f"Bjet.eta[:,{i}]",
      null_value=EMPTY_FLOAT,
      binning=(50, -2.5, 2.5),
      x_title=r"B-jet %i $\eta$" % (i + 1),
    )
    config.add_variable(
      name=f"bjet{i+1}_phi",
      expression=f"Bjet.phi[:,{i}]",
      null_value=EMPTY_FLOAT,
      binning=(40, -3.2, 3.2),
      x_title=r"B-jet %i $\phi$" % (i + 1),
    )
    config.add_variable(
      name=f"bjet{i+1}_mass",
      expression=f"Bjet.mass[:,{i}]",
      null_value=EMPTY_FLOAT,
      binning=(40, 0, 200),
      unit="GeV",
      x_title=r"B-jet %i mass" % (i + 1),
    )

  # LightJets (3 pt-leading lightjets)
  for i in range(6):
    config.add_variable(
      name=f"lightjet{i+1}_pt",
      expression=f"lightjets_pt[:,{i}]",
      null_value=EMPTY_FLOAT,
      binning=(40, 0., 400.),
      unit="GeV",
      x_title=r"lightjet %i $p_{T}$" % (i + 1),
    )
    config.add_variable(
      name=f"lightjet{i+1}_eta",
      expression=f"lightjets_eta[:,{i}]",
      null_value=EMPTY_FLOAT,
      binning=(50, -2.5, 2.5),
      x_title=r"lightjet %i $\eta$" % (i + 1),
    )
    config.add_variable(
      name=f"lightjet{i+1}_phi",
      expression=f"lightjets_phi[:,{i}]",
      null_value=EMPTY_FLOAT,
      binning=(40, -3.2, 3.2),
      x_title=r"lightjet %i $\phi$" % (i + 1),
    )
    config.add_variable(
      name=f"lightjet{i+1}_mass",
      expression=f"lightjets_mass[:,{i}]",
      null_value=EMPTY_FLOAT,
      binning=(40, 0, 200),
      unit="GeV",
      x_title=r"lightjet %i mass" % (i + 1),
    )

  config.add_variable(
      name="bjets_pt",
      expression="Bjet.pt",
      null_value=EMPTY_FLOAT,
      binning=(40, 0., 400.),
      unit="GeV",
      x_title="All b-jets $p_T$",
  )

  config.add_variable(
      name="m_bb",
      expression="m_bb",
      null_value=EMPTY_FLOAT,
      binning=(40, 0, 400),
      unit="GeV",
      x_title="m_bb",
      aux={"hist_axes": ("category", "process", "shift"),},
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
      name=f"whad_mass",
      expression="whad_mass",
      null_value=EMPTY_FLOAT,
      binning=(40, 0., 400.),
      unit="GeV",
      x_title=r"$m_{whad}$",
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

  config.add_variable(
      name="top_mass",
      expression="top_mass",
      null_value=EMPTY_FLOAT,
      binning=(40, 60., 600.),
      unit="GeV",
      x_title="top_mass",
  )
  
  config.add_variable(
      name="top_pt",
      expression="top_pt",
      null_value=EMPTY_FLOAT,
      binning=(40, 0., 400.),
      unit="GeV",
      x_title="top_pt",
  )

  config.add_variable(
      name="top_had_mass",
      expression="top_had_mass",
      null_value=EMPTY_FLOAT,
      binning=(40, 60., 600.),
      unit="GeV",
      x_title="top_had_mass",
  )
  
  config.add_variable(
      name="m_tt",
      expression="tt_bar_mass",
      null_value=EMPTY_FLOAT,
      binning=(40, 100., 1500.),
      unit="GeV",
      x_title="m_tt",
      aux={"hist_axes": ("category", "process", "shift"),},
  )
  
  config.add_variable(
      name="tt_pt",
      expression="tt_bar_pt",
      null_value=EMPTY_FLOAT,
      binning=(50, 100., 1000.),
      unit="GeV",
      x_title="tt_pt",
  )
    
  config.add_variable(
      name="lightjet_pt",
      expression="lightjets_pt",
      null_value=EMPTY_FLOAT,
      binning=(40, 0., 300.),
      unit="GeV",
      x_title="lightjet_pt",
  )

  config.add_variable(
      name="m_lead_b",
      expression="m_lead_b",
      null_value=EMPTY_FLOAT,
      binning=(40, 0., 300.),
      unit="GeV",
      x_title="mass of leading b",
  )

  config.add_variable(
      name="lead_b_pt",
      expression="lead_b_pt",
      null_value=EMPTY_FLOAT,
      binning=(40, 0., 300.),
      unit="GeV",
      x_title="pt of leading b",
  )

  config.add_variable(
      name="lightjet_mass",
      expression="lightjets_mass",
      null_value=EMPTY_FLOAT,
      binning=(40, 0., 300.),
      unit="GeV",
      x_title="lightjet_mass",
  )  

  config.add_variable(
      name="m_X",
      expression="m_X",
      null_value=EMPTY_FLOAT,
      binning=(50, 0., 1500.),
      unit="GeV",
      x_title="m_ttH",
      aux={"hist_axes": ("category", "process", "shift")},
  )  
  

#sh
  # config.add_variable(
  #     name="top_mass",
  #     expression="top_mass",
  #     null_value=EMPTY_FLOAT,
  #     binning=(40, 0., 400.),
  #     unit="GeV",
  #     x_title="top_mass",
  # )
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
      x_title=r"Number of electrons ($p_{T}$ > 30 GeV, $|\eta| < 2.4$ + tight Iso)",
  )
  config.add_variable(
      name="cf_n_muo",
      expression="cutflow.n_mu",
      binning=(5, -0.5, 4.5),
      x_title=r"Number of muons ($p_{T}$ > 30 GeV, $|\eta| < 2.4$ + tight Id)",
  )

  config.add_variable(
      name="deltaR_qq",
      expression="deltaR_qq",
      null_value=EMPTY_FLOAT,
      binning=(40, 0.0, 5.0),   # adjust binning (ΔR usually 0–5)
      x_title="ΔR(q, q)",
  )

  config.add_variable(
      name="deltaR_jj",
      expression="deltaR_jj",
      null_value=EMPTY_FLOAT,
      binning=(40, 0.0, 5.0),   # adjust binning (ΔR usually 0–5)
      x_title="ΔR(j, j)",
  )

  config.add_variable(
      name="deltaR_bb",
      expression="deltaR_bb",
      null_value=EMPTY_FLOAT,
      binning=(40, 0.0, 5.0),
      x_title="ΔR(b, b)",
  )


  # NN scores
  for model in MODEL_NAMES:
    config.add_variable(
        name=f"nn_score__{model}",
        expression=f"{model}.score",
        binning=(25, 0., 1.),
        unit="",
        x_title=f"NN score ({model})",
        aux={
            "hist_axes": ("category", "process", "shift"),
            "inputs": {f"{model}.score"},
        },
    )

  # Logit-transformed NN scores to emphasise score shape over a wider dynamic range
  for model in MODEL_NAMES:
    config.add_variable(
        name=f"logit_nn_score__{model}",
        expression=lambda events, model=model: _logit_transform(events[model]["score"]),
        null_value=-1.0,
        binning=(1000, -2., 12.),
        unit="",
        x_title=f"logit NN score ({model})",
        aux={
            "hist_axes": ("category", "process", "shift"),
            "inputs": {f"{model}.score"},
            "rebin": 25,
        },
    )


  # config.add_variable(
  #     name="deltaR_bb",
  #     expression="deltaR_bb",
  #     null_value=EMPTY_FLOAT,
  #     binning=(50, 0.0, 5.0),   # adjust binning (ΔR usually 0–5)
  #     x_title="ΔR(b, b)",
  # )

  # TODO: Could add here variables about cutflow in lepton_selection.py
  # See AZH as example
