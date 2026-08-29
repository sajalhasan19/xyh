# coding: utf-8

"""
Configuration of the Run 3 XYH analysis.
"""

from __future__ import annotations

import os
import re
from typing import Set

import yaml
from scinum import Number
import order as od
import law

from columnflow.util import DotDict
from columnflow.production.cms.btag import BTagSFConfig

from cmsdb.util import add_decay_process

from xyh.config.analysis_xyh import analysis_xyh
from xyh.config.categories import add_all_categories
from xyh.config.variables import add_variables
from columnflow.config_util import (
    get_root_processes_from_campaign, add_shift_aliases, get_shifts_from_sources,
)
from xyh.inference.signals import (
    XYH_SIGNAL_PROCESSES,
    XYH_SIGNAL_DATASETS,
)


thisdir = os.path.dirname(os.path.abspath(__file__))


def _add_process_children(
  cfg: od.Config,
  parent_name: str,
  child_names: list[str],
  available_processes: od.unique.UniqueObjectIndex | None = None,
) -> None:
  """
  Attach existing child processes to an existing parent process in the config.

  This is used to make generic process groups such as ``tt_nonb`` expand to the
  decay-split processes that are actually written into histogram process axes.
  """
  parent = cfg.get_process(parent_name)
  for child_name in child_names:
    child = cfg.get_process(child_name, default=None)
    if child is None:
      if available_processes is None:
        raise ValueError(f"unknown process: {child_name}")
      child = available_processes.get(child_name, default=None)
      if child is None:
        raise ValueError(f"unknown process: {child_name}")
      cfg.add_process(child)
      child = cfg.get_process(child_name)
    if child not in parent.processes:
      parent.add_process(child)


def add_config(
  analysis: od.Analysis,
  campaign: od.Campaign,
  config_name: str | None = None,
  config_id: int | None = None,
  limit_dataset_files: int | None = None,
) -> od.Config:
  assert campaign.x.year in [2022, 2023]
  if campaign.x.year == 2022:
    assert campaign.x.EE in ["pre", "post"]
  elif campaign.x.year == 2023:
    assert campaign.x.BPix in ["pre", "post"]

  # gather campaign data
  year = campaign.x.year
  year2 = year % 100
  corr_postfix = ""
  if year == 2022:
    corr_postfix = f"{campaign.x.EE}EE"
  elif year == 2023:
    corr_postfix = f"{campaign.x.BPix}BPix"

  implemented_years = [2022]

  if year not in implemented_years:
    raise NotImplementedError("For now, only 2022 campaign is fully implemented")

  limited_postee_excluded_datasets = set()
  if config_name == "config_2022post_limited":
    limited_postee_excluded_datasets.add("dy_m4to10_amcatnlo")

  # get all root processes
  procs = get_root_processes_from_campaign(campaign)

  # create a config by passing the campaign, so id and name will be identical
  cfg = analysis_xyh.add_config(campaign, name=config_name, id=config_id)
  cfg.x.run = cfg.campaign.x.run
  cfg.x.jet_pt = 30

  colors = {
    "dy": "#FBFF36",
    "data": "#000000",
    "tt": "#E04F21",  # red
    "tt_nonb": "#E04F21",  # red
    "tt_1b": "#B22222",  # firebrick
    "tt_hf": "#8B0000",  # dark red
    "ttbb": "#9E1B1B",  # dark red
    "ttv": "#5E8FFC",  # blue
    "w_lnu": "#82FF28",  # green
    "st": "#3E00FB",  # dark purple
    "ttvv": "#A020F0",  # purple
    "xyh": "#000000",  # default for signal hypotheses
    "ww": "#FFAA00",  # orange
    "wz": "#00AAAA",  # cyan
    "zz": "#FF00FF",  # magenta
  }

  # add datasets we need to study
  process_names = [
    "dy",
    "tt_nonb",
    "tt_1b",
    "tt_hf",
    "ttbb",
    "ttv",
    "ttvv",
    "st",
    "w_lnu",
    "data",
    "ww",
    "wz",
    "zz",
  ]

  process_names.extend(XYH_SIGNAL_PROCESSES)
  signal_processes: list[str] = []
  background_processes: list[str] = []

  for process_name in process_names:
    process_obj = procs.get(process_name, default=None)
    if process_obj is None:
      if process_name.startswith("xyh_sl_"):
        continue
      raise ValueError(f"Process '{process_name}' not defined in campaign '{campaign.name}'")
    cfg.add_process(process_obj)
    color_key = "xyh" if process_name.startswith("xyh_sl_") else process_name
    cfg.get_process(process_name).color1 = colors.get(color_key, "#aaaaaa")
    cfg.get_process(process_name).color2 = colors.get(color_key, "#000000")
    if process_name.startswith("xyh_sl_"):
      if process_name not in signal_processes:
        signal_processes.append(process_name)
    elif process_name != "data":
      if process_name not in background_processes:
        background_processes.append(process_name)

  # Keep legacy decay-split top processes in the config as support processes.
  # Event processing and histogram process axes can still carry the original
  # inclusive ids (tt_sl / tt_dl / tt_fh and ttbb_sl / ttbb_dl / ttbb_fh),
  # while higher-level plotting and inference should use the grouped top
  # processes defined above.  The removed inclusive tt+HF component has its
  # own tt_hf support process so plain tt remains the full inclusive parent.
  for process_name in [
    "tt",
    "tt_sl",
    "tt_dl",
    "tt_fh",
    "ttbb_sl",
    "ttbb_dl",
    "ttbb_fh",
  ]:
    process_obj = procs.get(process_name, default=None)
    if process_obj is not None and cfg.get_process(process_name, default=None) is None:
      cfg.add_process(process_obj)
      cfg.get_process(process_name).color1 = colors.get("ttbb" if process_name.startswith("ttbb") else "tt", "#aaaaaa")
      cfg.get_process(process_name).color2 = colors.get("ttbb" if process_name.startswith("ttbb") else "tt", "#000000")

  _add_process_children(cfg, "tt", ["tt_sl", "tt_dl", "tt_fh"], procs)
  _add_process_children(cfg, "ttbb", ["ttbb_sl", "ttbb_dl", "ttbb_fh"], procs)

  # Re-attach decay-split top subprocesses to the generic top process groups so
  # tasks using walk_processes(), e.g. CreateYieldTable, can resolve histograms
  # whose process axes contain names like tt_sl_nonb instead of tt_nonb.
  _add_process_children(cfg, "tt_nonb", ["tt_dl_nonb", "tt_sl_nonb", "tt_fh_nonb"], procs)
  _add_process_children(cfg, "tt_1b", ["tt_dl_1b", "tt_sl_1b", "tt_fh_1b"], procs)
  _add_process_children(cfg, "tt_hf", ["tt_dl_hf", "tt_sl_hf", "tt_fh_hf"], procs)
  _add_process_children(cfg, "ttbb_nonb", ["ttbb_dl_nonb", "ttbb_sl_nonb", "ttbb_fh_nonb"], procs)
  _add_process_children(cfg, "ttbb_1b", ["ttbb_dl_1b", "ttbb_sl_1b", "ttbb_fh_1b"], procs)

  def _match_era(
    *,
    run: int | set[int] | None = None,
    year: int | set[int] | None = None,
    postfix: str | set[int] | None = None,
    tag: str | set[str] | None = None,
    nano: int | set[int] | None = None,
    sync: bool = False,
  ) -> bool:
    return (
      (run is None or campaign.x.run in law.util.make_set(run)) and
      (year is None or campaign.x.year in law.util.make_set(year)) and
      (postfix is None or campaign.x.postfix in law.util.make_set(postfix)) and
      (tag is None or campaign.has_tag(tag, mode=any)) and
      (nano is None or campaign.x.version in law.util.make_set(nano))
    )

  def if_era(*, values: list[str | None] | None = None, **kwargs) -> list[str]:
    return list(filter(bool, values or [])) if _match_era(**kwargs) else []

  def if_not_era(*, values: list[str | None] | None = None, **kwargs) -> list[str]:
    return list(filter(bool, values or [])) if not _match_era(**kwargs) else []

  dataset_names = [
    # # TT
    "tt_sl_powheg",
    "tt_dl_powheg",
    "tt_fh_powheg",
    "ttbb_sl_powheg",
    "ttbb_dl_powheg",
    "ttbb_fh_powheg",

    # # TTV
    # "ttz_zll_m4to50_amcatnlo",
    # "ttz_zll_m50toinf_amcatnlo",
    # "ttz_znunu_amcatnlo",
    # "ttz_zqq_amcatnlo",

    # # ST
    # TODO: Check these against bbWW
    "st_tchannel_tbar_4f_powheg",
    "st_tchannel_t_4f_powheg",  
    "st_twchannel_t_dl_powheg",
    "st_twchannel_t_fh_powheg",
    "st_twchannel_t_sl_powheg",
    "st_twchannel_tbar_fh_powheg",
    "st_twchannel_tbar_sl_powheg",
    "st_twchannel_tbar_dl_powheg",

    # # DY
    # NLO Samples
    # TODO: Implement stitching
    # "dy_m50toinf_amcatnlo",
    *if_era(year=2022, tag="preEE", values=[
      "dy_m10to50_amcatnlo",
      "dy_m4to10_amcatnlo",
      # "dy_m50toinf_amcatnlo",
      "dy_m50toinf_0j_amcatnlo",
      "dy_m50toinf_1j_amcatnlo",
      "dy_m50toinf_2j_amcatnlo",
    ]),
    *if_era(year=2022, tag="postEE", values=[
      "dy_m10to50_amcatnlo",
      "dy_m4to10_amcatnlo",
      #"dy_m50toinf_amcatnlo",
      "dy_m50toinf_0j_amcatnlo",
      "dy_m50toinf_1j_amcatnlo",
      "dy_m50toinf_2j_amcatnlo",
    ]),

    # # VV
    "zz_pythia",
    "ww_pythia",
    "wz_pythia",

    # # TTV
    "ttz_zqq_1j_amcatnlo",

    # # TTVV
    "ttww_madgraph",
    "ttzz_madgraph",

    # # WLNU
    "w_lnu_amcatnlo",



    # # Data
    # # Double Muon
    *if_era(year=2022, tag="preEE", values=[
      "data_mu_c",
      "data_mu_d",
      "data_egamma_c",
      "data_egamma_d",
      "data_muoneg_c",
      "data_muoneg_d",
    ]),
    *if_era(year=2022, tag="postEE", values=[
      "data_mu_e",
      "data_mu_f",
      "data_mu_g",
      "data_egamma_e",
      "data_egamma_f",
      "data_egamma_g",
      "data_muoneg_e",
    ]),
  ]

  dataset_names.extend(XYH_SIGNAL_DATASETS)
  if limited_postee_excluded_datasets:
    dataset_names = [
      dataset_name
      for dataset_name in dataset_names
      if dataset_name not in limited_postee_excluded_datasets
    ]

  signal_datasets: list[str] = []
  background_datasets: list[str] = []

  for dataset_name in dataset_names:
    dataset_obj = campaign.datasets.get(dataset_name, default=None)
    if dataset_obj is None:
      if dataset_name.startswith("xyh_sl_"):
        continue
      raise ValueError(f"Dataset '{dataset_name}' not defined in campaign '{campaign.name}'")
    dataset = cfg.add_dataset(dataset_obj)
    if limit_dataset_files:
      # apply optional limit on the max. number of files per dataset
      for info in dataset.info.values():
        if info.n_files > limit_dataset_files:
          info.n_files = limit_dataset_files
    if dataset.name.startswith("tt"):
      dataset.add_tag({"is_ttbar"})
    if dataset.name.startswith("dy"):
      dataset.add_tag({"is_dy"})
    if dataset.name.startswith("xyh_sl_"):
      if dataset.name not in signal_datasets:
        signal_datasets.append(dataset.name)
    elif not dataset.name.startswith("data_"):
      if dataset.name not in background_datasets:
        background_datasets.append(dataset.name)
    # TODO: Add signal


  # default calibrator, selector, producer, ml model and inference model
  cfg.x.default_calibrator = "default" # "skip_jecunc" TODO: use this one?
  cfg.x.default_selector = "default"
  cfg.x.default_reducer = "cf_default"
  cfg.x.default_producer = "default"
  cfg.x.default_hist_producer = "all_weights"
  cfg.x.default_weight_producer = "all_weights"
  cfg.x.default_ml_model = None
  cfg.x.default_inference_model = "xyh_limits"
  cfg.x.default_categories = ["cat_incl"]
  cfg.x.default_variables = ["jet1_pt"]

  cfg.x.default_bins_per_category = {
    # categories
    # "cat_incl": 20,
    # "1lep__2bjets__4jets": 15,
    # "1lep__2bjets__5jets": 15,
    # "1lep__2bjets__6jets": 15,
    # "1lep__2bjets__g6jets": 10,
    "1lep__3bjets__4jets": 6,
    "1lep__3bjets__5jets": 6,
    #"1lep__3bjets__6jets": 6,
    "1lep__3bjets__ge6jets": 6,
    "1lep__4bjets__5jets": 6,
    "1lep__ge4bjets__ge6jets": 6,
    # # muon categories
    # "1mu__2bjets__4jets": 3,
    # "1mu__2bjets__5jets": 3,
    # "1mu__2bjets__6jets": 3,
    # "1mu__2bjets__g6jets": 3,
    # "1mu__3bjets__4jets": 4,
    # "1mu__3bjets__5jets": 4,
    # "1mu__3bjets__6jets": 4,
    # "1mu__3bjets__g6jets": 4,
    # "1mu__4bjets__5jets": 5,
    # "1mu__4bjets__6jets": 5,
    # "1mu__4bjets__g6jets": 5,
    # "1mu__5bjets__6jets": 2,
    # "1mu__5bjets__g6jets": 2,
  }

  # disable histogram blinding during rebinning to optimize bin edges for MC-only studies
  cfg.x.disable_rebin_blinding = True

  # cfg.x.min_bkg_events_per_category = {
  #   # 2b control regions: keep bins smooth
  #   "1e__2bjets__4jets": 30,
  #   "1e__2bjets__5jets": 30,
  #   "1e__2bjets__6jets": 20,
  #   "1e__2bjets__g6jets": 20,
  #   "1mu__2bjets__4jets": 30,
  #   "1mu__2bjets__5jets": 30,
  #   "1mu__2bjets__6jets": 20,
  #   "1mu__2bjets__g6jets": 20,
  #   # 3b categories
  #   "1e__3bjets__4jets": 15,
  #   "1e__3bjets__5jets": 12,
  #   "1e__3bjets__6jets": 8,
  #   "1e__3bjets__g6jets": 8,
  #   "1mu__3bjets__4jets": 15,
  #   "1mu__3bjets__5jets": 12,
  #   "1mu__3bjets__6jets": 8,
  #   "1mu__3bjets__g6jets": 8,
  #   # 4b / 5b signal-rich categories allow almost empty bins
  #   "1e__4bjets__5jets": 2,
  #   "1e__4bjets__6jets": 2,
  #   "1e__4bjets__g6jets": 2,
  #   "1mu__4bjets__5jets": 2,
  #   "1mu__4bjets__6jets": 2,
  #   "1mu__4bjets__g6jets": 1,
  #   "1e__5bjets__6jets": 1,
  #   "1e__5bjets__g6jets": 1,
  #   "1mu__5bjets__6jets": 1,
  #   "1mu__5bjets__g6jets": 1,
  # }


  is_xyh_signal = lambda proc_name: proc_name.startswith("xyh_sl_")
  is_xyh_background = lambda proc_name: proc_name.upper() in {
    "TT_NONB",
    "TTBB_1B",
    "DY",
    "ST",
    "W_LNU",
    "TTV",
    "TTVV",
    "VV",
    "WW",
    "WZ",
    "ZZ",
  }
  only_process = lambda target: (lambda proc_name: proc_name.lower() == target.lower())

  background_rebin_categories = [
    # "1lep__2bjets__4jets",
    # "1lep__2bjets__5jets",
    # "1lep__2bjets__6jets",
    # "1lep__2bjets__g6jets",
    
  ]

  signal_rebin_categories = [
    "1lep__3bjets__4jets",
    #"1lep__3bjets__6jets",
    "1lep__3bjets__ge6jets",
    "1lep__4bjets__5jets",
    "1lep__ge4bjets__ge6jets",
    "1lep__3bjets__5jets",
  ]

  cfg.x.inference_category_rebin_processes = {
    **{cat: is_xyh_signal for cat in background_rebin_categories},
    **{cat: is_xyh_signal for cat in signal_rebin_categories},
  }

  tt_control_rebin_categories = [
    # "1lep__2bjets__4jets",
    # "1lep__2bjets__5jets",
    # "1lep__2bjets__6jets",
    # "1lep__2bjets__g6jets",
  ]
  for cat in tt_control_rebin_categories:
    cfg.x.inference_category_rebin_processes[cat] = is_xyh_signal



  # process groups for conveniently looping over certain processs
  # (used in wrapper_factory and during plotting)
  cfg.x.process_groups = {
    "all": ["*"],
  }
  if background_processes:
    cfg.x.process_groups["background"] = background_processes
  if signal_processes:
    cfg.x.process_groups["signals"] = signal_processes

  # dataset groups for conveniently looping over certain datasets
  # (used in wrapper_factory and during plotting)
  cfg.x.dataset_groups = {
    "all": ["*"],
  }
  if background_datasets:
    cfg.x.dataset_groups["background"] = background_datasets
  if signal_datasets:
    cfg.x.dataset_groups["signals"] = signal_datasets

  # category groups for conveniently looping over certain categories
  # (used during plotting)
  cfg.x.category_groups = {
    "default": ["incl"],
  }

  # variable groups for conveniently looping over certain variables
  # (used during plotting)
  cfg.x.variable_groups = {
    "default": ["n_jet", "jet1_pt"],
  }

  # # shift groups for conveniently looping over certain shifts
  # # (used during plotting)
  # cfg.x.shift_groups = {
  #   "jer": ["nominal", "jer_up", "jer_down"],
  # }

  cfg.x.shift_groups = {
  "jec": ["nominal", "jec_Total_up", "jec_Total_down"],
  "jer": ["nominal", "jer_up", "jer_down"],
  "jme": ["nominal", "jec_Total_up", "jec_Total_down", "jer_up", "jer_down"],
  }

  # selector step groups for conveniently looping over certain steps
  # (used in cutflow tasks)
  cfg.x.selector_step_groups = {} # TODO

  cfg.x.selector_step_labels = {
    "json": r"JSON",
    "trigger": r"Trigger",
    "met_filter": r"MET filters",
  }

  # plotting settings groups
  cfg.x.general_settings_groups = {
    "default_norm": {"shape_norm": True, "yscale": "log"},
  }
  cfg.x.process_settings_groups = {
    "Jet": r"$N_{jets}^{AK4} \geq 3$",
  }
  # when drawing DY as a line, use a different type of yellow

  cfg.x.variable_settings_groups = {}

  # lumi values in inverse pb
  # https://twiki.cern.ch/twiki/bin/view/CMS/LumiRecommendationsRun2?rev=2#Combination_and_correlations
  if year == 2022:
    if campaign.x.EE == "pre":
      cfg.x.luminosity = Number(7971, {
        "lumi_13TeV_2022": 0.01j,
        "lumi_13TeV_correlated": 0.006j,
      })
    elif campaign.x.EE == "post":
      cfg.x.luminosity = Number(26337, {
        "lumi_13TeV_2022": 0.01j,
        "lumi_13TeV_correlated": 0.006j,
      })
  else:
    raise NotImplementedError(f"Luminosity for year {year} is not defined.")

  # MET filters
  # TODO: Different Met filters for different years
  # https://twiki.cern.ch/twiki/bin/view/CMS/MissingETOptionalFiltersRun2?rev=158#2018_2017_data_and_MC_UL
  cfg.x.met_filters = {
    "Flag.goodVertices",
    "Flag.globalSuperTightHalo2016Filter",
    "Flag.HBHENoiseFilter",
    "Flag.HBHENoiseIsoFilter",
    "Flag.EcalDeadCellTriggerPrimitiveFilter",
    "Flag.BadPFMuonFilter",
    "Flag.BadPFMuonDzFilter",
    "Flag.eeBadScFilter",
    "Flag.ecalBadCalibFilter",
  }

  # minimum bias cross section in mb (milli) for creating PU weights, values from
  # https://twiki.cern.ch/twiki/bin/view/CMS/PileupJSONFileforData?rev=45#Recommended_cross_section
  cfg.x.minbias_xs = Number(69.2, 0.046j)

  # whether to validate the number of obtained LFNs in GetDatasetLFNs
  cfg.x.validate_dataset_lfns = True

  # jec configuration
  # https://twiki.cern.ch/twiki/bin/view/CMS/JECDataMC?rev=201
  jerc_postfix = ""
  if year == 2022 and campaign.x.EE == "post":
    jerc_postfix = "EE"

  jerc_campaign = f"Summer{year2}{jerc_postfix}_22Sep2023"
  jet_type = "AK4PFPuppi"

  cfg.x.jec = DotDict.wrap({
    "Jet": {
      "campaign": jerc_campaign,
      "version": {2016: "V7", 2017: "V5", 2018: "V5", 2022: "V2"}[year],
      "jet_type": jet_type,
      "levels": ["L1FastJet", "L2Relative", "L2L3Residual", "L3Absolute"],
      "levels_for_type1_met": ["L1FastJet"],
      "uncertainty_sources": [
          "Total",
      ],
    },
  })

  # JER
  # https://twiki.cern.ch/twiki/bin/view/CMS/JetResolution?rev=107
  cfg.x.jer = DotDict.wrap({
    "Jet": {
      "campaign": jerc_campaign,
      "version": {2022: "JRV1"}[year],
      "jet_type": jet_type,
    },
  })

  # JEC uncertainty sources propagated to btag scale factors
  # (names derived from contents in BTV correctionlib file)
  cfg.x.btag_sf_jec_sources = [
    "",  # total
    "Absolute",
    "AbsoluteMPFBias",
    "AbsoluteScale",
    "AbsoluteStat",
    f"Absolute_{year}",
    "BBEC1",
    f"BBEC1_{year}",
    "EC2",
    f"EC2_{year}",
    "FlavorQCD",
    "Fragmentation",
    "HF",
    f"HF_{year}",
    "PileUpDataMC",
    "PileUpPtBB",
    "PileUpPtEC1",
    "PileUpPtEC2",
    "PileUpPtHF",
    "PileUpPtRef",
    "RelativeBal",
    "RelativeFSR",
    "RelativeJEREC1",
    "RelativeJEREC2",
    "RelativeJERHF",
    "RelativePtBB",
    "RelativePtEC1",
    "RelativePtEC2",
    "RelativePtHF",
    "RelativeSample",
    f"RelativeSample_{year}",
    "RelativeStatEC",
    "RelativeStatFSR",
    "RelativeStatHF",
    "SinglePionECAL",
    "SinglePionHCAL",
    "TimePtEta",
  ]

  # b-tag working points
  # https://btv-wiki.docs.cern.ch/ScaleFactors/Run3Summer22/
  # https://btv-wiki.docs.cern.ch/ScaleFactors/Run3Summer22EE/
  # TODO: add correct 2022 + 2022preEE WP for deepcsv if needed
  btag_key = f"2022{campaign.x.EE}EE" if year == 2022 else year
  cfg.x.btag_working_points = DotDict.wrap({
    "deepjet": {
      "loose": {
        "2022preEE": 0.0583, "2022postEE": 0.0614,
      }[btag_key],
      "medium": {
        "2022preEE": 0.3086, "2022postEE": 0.3196,
      }[btag_key],
      "tight": {
        "2022preEE": 0.7183, "2022postEE": 0.7300,
      }[btag_key],
    },
    "deepcsv": {
      "loose": {
        "2022preEE": 0.1208, "2022postEE": 0.1208,
      }[btag_key],
      "medium": {
        "2022preEE": 0.4168, "2022postEE": 0.4168,
      }[btag_key],
      "tight": {
        "2022preEE": 0.7665, "2022postEE": 0.7665,
      }[btag_key],
    },
  })

  # b-tag configuration. Potentially overwritten by the jet Selector.
  if cfg.x.run == 2:
    cfg.x.b_tagger = "deepjet"
    cfg.x.btag_sf = BTagSFConfig(
      correction_set="deepJet_shape",
      jec_sources=cfg.x.btag_sf_jec_sources,
      discriminator="btagDeepFlavB",
    )
  elif cfg.x.run == 3:
    cfg.x.b_tagger = "deepjet" # TODO : Move to pnet?
    cfg.x.btag_sf = BTagSFConfig(
      correction_set="deepJet_shape",
      jec_sources=cfg.x.btag_sf_jec_sources,
      discriminator="btagDeepFlavB",
    )

  cfg.x.btag_column = cfg.x.btag_sf.discriminator
  cfg.x.btag_wp = "medium"
  cfg.x.btag_wp_score = (
    cfg.x.btag_working_points[cfg.x.b_tagger][cfg.x.btag_wp]
  )
  if cfg.x.btag_wp_score == 0.0:
    raise ValueError(f"Unknown b-tag working point '{cfg.x.btag_wp}' for campaign {cfg.x.cpn_tag}")
  # TODO: Add pnet
  # cfg.x.xbb_btag_wp_score = cfg.x.btag_working_points["particlenet_xbb_vs_qcd"]["medium"]
  # if cfg.x.xbb_btag_wp_score == 0.0:
  #   raise ValueError(f"Unknown xbb b-tag working point 'medium' for campaign {cfg.x.cpn_tag}")

  # names of electron correction sets and working points
  # (used in the electron_sf producer)
  if f"{year}{corr_postfix}" == "2022postEE":
    cfg.x.electron_sf_names = ("Electron-ID-SF", "2022Re-recoE+PromptFG", "RecoAbove75")
    cfg.x.electron_sf_mid_names = ("Electron-ID-SF", "2022Re-recoE+PromptFG", "Reco20to75")
    cfg.x.electron_sf_id_names = ("Electron-ID-SF", "2022Re-recoE+PromptFG", "wp80iso")
  elif f"{year}{corr_postfix}" == "2022preEE":
    cfg.x.electron_sf_names = ("Electron-ID-SF", "2022Re-recoBCD", "RecoAbove75")
    cfg.x.electron_sf_mid_names = ("Electron-ID-SF", "2022Re-recoBCD", "Reco20to75")
    cfg.x.electron_sf_id_names = ("Electron-ID-SF", "2022Re-recoBCD", "wp80iso")
  # names of muon correction sets and working points
  # (used in the muon producer)
  cfg.x.muon_sf_names = ("NUM_TightRelTkIso_DEN_HighPtID", f"{year}{corr_postfix}")
  cfg.x.muon_sf_id_names = ("NUM_HighPtID_DEN_TrackerMuons", f"{year}{corr_postfix}")
  cfg.x.muon_sf_iso_names = ("NUM_TightRelTkIso_DEN_HighPtID", f"{year}{corr_postfix}")

  cfg.x.top_pt_weight = {
    "a": 0.0615,
    "b": -0.0005,
  }

  # helper to add column aliases for both shifts of a source
  def add_aliases(shift_source: str, aliases: Set[str], selection_dependent: bool):
    for direction in ["up", "down"]:
      shift = cfg.get_shift(od.Shift.join_name(shift_source, direction))
      # format keys and values
      inject_shift = lambda s: re.sub(r"\{([^_])", r"{_\1", s).format(**shift.__dict__)
      _aliases = {inject_shift(key): inject_shift(value) for key, value in aliases.items()}
      alias_type = "column_aliases_selection_dependent" if selection_dependent else "column_aliases"
      # extend existing or register new column aliases
      shift.set_aux(alias_type, shift.get_aux(alias_type, {})).update(_aliases)

  # register shifts
  # TODO: make shifts year-dependent
  cfg.add_shift(name="nominal", id=0)
  # cfg.add_shift(name="tune_up", id=1, type="shape", tags={"disjoint_from_nominal"})
  # cfg.add_shift(name="tune_down", id=2, type="shape", tags={"disjoint_from_nominal"})
  # cfg.add_shift(name="hdamp_up", id=3, type="shape", tags={"disjoint_from_nominal"})
  # cfg.add_shift(name="hdamp_down", id=4, type="shape", tags={"disjoint_from_nominal"})
  cfg.add_shift(name="minbias_xs_up", id=7, type="shape")
  cfg.add_shift(name="minbias_xs_down", id=8, type="shape")
  add_aliases("minbias_xs", {"pu_weight": "pu_weight_{name}"}, selection_dependent=False)
  cfg.add_shift(name="top_pt_up", id=9, type="shape")
  cfg.add_shift(name="top_pt_down", id=10, type="shape")
  add_aliases("top_pt", {"top_pt_weight": "top_pt_weight_{direction}"}, selection_dependent=False)

  cfg.add_shift(name="e_sf_up", id=40, type="shape")
  cfg.add_shift(name="e_sf_down", id=41, type="shape")
  # Trigger SFs are disabled while no trigger selection is applied.
  # cfg.add_shift(name="e_trig_sf_up", id=42, type="shape")
  # cfg.add_shift(name="e_trig_sf_down", id=43, type="shape")
  add_aliases(
    "e_sf",
    {
      "electron_weight": "electron_weight_{direction}",
      "electron_mid_weight": "electron_mid_weight_{direction}",
      "electron_id_weight": "electron_id_weight_{direction}",
    },
    selection_dependent=False,
  )
  # add_aliases("e_trig_sf", {"electron_weight": "electron_weight_{direction}"}, selection_dependent=False)

  cfg.add_shift(name="muon_up", id=51, type="shape")
  cfg.add_shift(name="muon_down", id=52, type="shape")
  add_aliases(
    "muon",
    {
      "muon_id_weight": "muon_id_weight_{direction}",
      "muon_iso_weight": "muon_iso_weight_{direction}",
    },
    selection_dependent=False,
  )

  # b-tag shape uncertainty sources
  btag_uncs = [
    ("cferr1", "cferr1"),
    ("cferr2", "cferr2"),
    ("hf", "hf"),
    ("hfstats1", f"hfstats1_{year}"),
    ("hfstats2", f"hfstats2_{year}"),
    ("lf", "lf"),
    ("lfstats1", f"lfstats1_{year}"),
    ("lfstats2", f"lfstats2_{year}"),
  ]
  for i, (unc, btag_weight_label) in enumerate(btag_uncs):
    shift_source = f"btag_{unc}"
    cfg.add_shift(name=f"{shift_source}_up", id=100 + 2 * i, type="shape")
    cfg.add_shift(name=f"{shift_source}_down", id=101 + 2 * i, type="shape")
    add_aliases(
      shift_source,
      {"btag_weight": f"btag_weight_{btag_weight_label}" + "_{direction}"},
      selection_dependent=False,
    )

  # cfg.add_shift(name="mur_up", id=201, type="shape")
  # cfg.add_shift(name="mur_down", id=202, type="shape")
  # cfg.add_shift(name="muf_up", id=203, type="shape")
  # cfg.add_shift(name="muf_down", id=204, type="shape")
  cfg.add_shift(name="murf_envelope_up", id=205, type="shape")
  cfg.add_shift(name="murf_envelope_down", id=206, type="shape")
  cfg.add_shift(name="pdf_up", id=207, type="shape")
  cfg.add_shift(name="pdf_down", id=208, type="shape")

  # add_aliases("mur", {"mur_weight": "mur_weight_{direction}"}, selection_dependent=False)
  # add_aliases("muf", {"muf_weight": "muf_weight_{direction}"}, selection_dependent=False)
  add_aliases(
    "murf_envelope",
    {"murmuf_envelope_weight": "murmuf_envelope_weight_{direction}"},
    selection_dependent=False,
  )
  add_aliases("pdf", {"pdf_weight": "pdf_weight_{direction}"}, selection_dependent=False)

  # cfg.add_shift(name="jer_up", id=6000, type="shape", tags={"selection_dependent"})
  # cfg.add_shift(name="jer_down", id=6001, type="shape", tags={"selection_dependent"})
  # add_aliases("jer", {"Jet.pt": "Jet.pt_{name}", "Jet.mass": "Jet.mass_{name}"}, selection_dependent=True)

  #
  # # Jet energy scale and resolution shape uncertainties
  # #

  # # JEC uncertainty sources.
  # # With cfg.x.jec.Jet.uncertainty_sources = ["Total"], this creates:
  # #   jec_Total_up
  # #   jec_Total_down
  # for i, jec_source in enumerate(cfg.x.jec.Jet.uncertainty_sources):
  #   shift_source = f"jec_{jec_source}"

  #   cfg.add_shift(
  #     name=f"{shift_source}_up",
  #     id=5000 + 2 * i,
  #     type="shape",
  #     tags={"selection_dependent"},
  #   )
  #   cfg.add_shift(
  #     name=f"{shift_source}_down",
  #     id=5001 + 2 * i,
  #     type="shape",
  #     tags={"selection_dependent"},
  #   )

  #   add_aliases(
  #     shift_source,
  #     {
  #       "Jet.pt": "Jet.pt_{name}",
  #       "Jet.mass": "Jet.mass_{name}",
  #     },
  #     selection_dependent=True,
  #   )

  # # JER uncertainty
  # cfg.add_shift(
  #   name="jer_up",
  #   id=6000,
  #   type="shape",
  #   tags={"selection_dependent"},
  # )
  # cfg.add_shift(
  #   name="jer_down",
  #   id=6001,
  #   type="shape",
  #   tags={"selection_dependent"},
  # )

  # add_aliases(
  #   "jer",
  #   {
  #     "Jet.pt": "Jet.pt_{name}",
  #     "Jet.mass": "Jet.mass_{name}",
  #   },
  #   selection_dependent=True,
  # )

    #
  # Jet energy scale and resolution shape uncertainties
  #

  # JEC uncertainty sources.
  # With cfg.x.jec.Jet.uncertainty_sources = ["Total"], this creates:
  #   jec_Total_up
  #   jec_Total_down
  for i, jec_source in enumerate(cfg.x.jec.Jet.uncertainty_sources):
    shift_source = f"jec_{jec_source}"

    cfg.add_shift(
      name=f"{shift_source}_up",
      id=5000 + 2 * i,
      type="shape",
      tags={"selection_dependent", "jec"},
    )
    cfg.add_shift(
      name=f"{shift_source}_down",
      id=5001 + 2 * i,
      type="shape",
      tags={"selection_dependent", "jec"},
    )

    # Required by the b-tag SF producer for shifts tagged as "jec".
    for direction in ["up", "down"]:
      shift = cfg.get_shift(od.Shift.join_name(shift_source, direction))
      shift.set_aux("jec_source", jec_source)

    # Object kinematic aliases for the selection-dependent JEC shift
    add_aliases(
      shift_source,
      {
        "Jet.pt": "Jet.pt_{name}",
        "Jet.mass": "Jet.mass_{name}",
        "MET.pt": "MET.pt_{name}",
        "MET.phi": "MET.phi_{name}",
      },
      selection_dependent=True,
    )

    # B-tag SF alias for the JEC-dependent b-tag weight
    add_aliases(
      shift_source,
      {
        "btag_weight": f"btag_weight_jec_{jec_source}" + "_{direction}",
      },
      selection_dependent=False,
    )

  # JER uncertainty
  cfg.add_shift(
    name="jer_up",
    id=6000,
    type="shape",
    tags={"selection_dependent", "jer"},
  )
  cfg.add_shift(
    name="jer_down",
    id=6001,
    type="shape",
    tags={"selection_dependent", "jer"},
  )

  add_aliases(
    "jer",
    {
      "Jet.pt": "Jet.pt_{name}",
      "Jet.mass": "Jet.mass_{name}",
      "MET.pt": "MET.pt_{name}",
      "MET.phi": "MET.phi_{name}",
    },
    selection_dependent=True,
  )

  def make_jme_filename(jme_aux, sample_type, name, era=None):
    """
    Convenience function to compute paths to JEC files.
    """
    # normalize and validate sample type
    sample_type = sample_type.upper()
    if sample_type not in ("DATA", "MC"):
      raise ValueError(f"invalid sample type '{sample_type}', expected either 'DATA' or 'MC'")

    jme_full_version = "_".join(s for s in (jme_aux.campaign, era, jme_aux.version, sample_type) if s)

    return f"{jme_aux.source}/{jme_full_version}/{jme_full_version}_{name}_{jme_aux.jet_type}.txt"

  # external files
  json_mirror = "/afs/cern.ch/user/j/jmatthie/public/mirrors/jsonpog-integration-49ddc547"
  local_repo = "/data/dust/user/matthiej/topsf"  # TODO: avoid hardcoding path

  corr_tag = f"{year}_Summer22{jerc_postfix}"

  cfg.x.external_files = DotDict.wrap({
    # pileup weight corrections
    "pu_sf": (f"{json_mirror}/POG/LUM/{corr_tag}/puWeights.json.gz", "v1"),

    # jet energy correction
    "jet_jerc": (f"{json_mirror}/POG/JME/{corr_tag}/jet_jerc.json.gz", "v1"),

    # electron scale factors
    "electron_sf": (f"{json_mirror}/POG/EGM/{corr_tag}/electron.json.gz", "v1"),

    # muon scale factors
    "muon_sf": (f"{json_mirror}/POG/MUO/{corr_tag}/muon_Z.json.gz", "v1"),

    # btag scale factor
    "btag_sf_corr": (f"{json_mirror}/POG/BTV/{corr_tag}/btagging.json.gz", "v1"),

    # V+jets reweighting
    #"vjets_reweighting": f"{local_repo}/data/json/vjets_reweighting.json.gz",

    # jet veto map
    "jet_veto_map": (f"{json_mirror}/POG/JME/{corr_tag}/jetvetomaps.json.gz", "v1")
  })

  # external files with more complex year dependence
  # TODO: generalize to different years

  if year == 2022 and campaign.x.EE == "pre":
    cfg.x.external_files.update(DotDict.wrap({
      # files from https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideGoodLumiSectionsJSONFile
      "lumi": {
        "golden": ("https://cms-service-dqmdc.web.cern.ch/CAF/certification/Collisions22/Cert_Collisions2022_355100_362760_Golden.json", "v1"),  # noqa
        "normtag": ("/afs/cern.ch/user/l/lumipro/public/Normtags/normtag_PHYSICS.json", "v1"),
      },
      "pu": {
        "json": (f"https://cms-service-dqmdc.web.cern.ch/CAF/certification/Collisions22/PileUp/BCDEFG/pileup_JSON.txt", "v1"),  # noqa
        "mc_profile": ("https://raw.githubusercontent.com/cms-sw/cmssw/bb525104a7ddb93685f8ced6fed1ab793b2d2103/SimGeneral/MixingModule/python/Run3_2022_LHC_Simulation_10h_2h_cfi.py", "v1"),  # noqa
        "data_profile": {
          "nominal": (f"/afs/cern.ch/user/a/anhaddad/public/Collisions22/pileupHistogram-Cert_Collisions2022_355100_362760_GoldenJson-13p6TeV-69200ub-100bins.root", "v1"),  # noqa
          "minbias_xs_up": (f"/afs/cern.ch/user/a/anhaddad/public/Collisions22/pileupHistogram-Cert_Collisions2022_355100_362760_GoldenJson-13p6TeV-72400ub-100bins.root", "v1"),  # noqa
          "minbias_xs_down": (f"/afs/cern.ch/user/a/anhaddad/public/Collisions22/pileupHistogram-Cert_Collisions2022_355100_362760_GoldenJson-13p6TeV-66000ub-100bins.root", "v1"),  # noqa
        },
      },
    }))
  elif year == 2022 and campaign.x.EE == "post":
    cfg.x.external_files.update(DotDict.wrap({
      # files from https://twiki.cern.ch/twiki/bin/view/CMSPublic/SWGuideGoodLumiSectionsJSONFile
      "lumi": {
        "golden": ("https://cms-service-dqmdc.web.cern.ch/CAF/certification/Collisions22/Cert_Collisions2022_355100_362760_Golden.json", "v1"),  # noqa
        "normtag": ("/afs/cern.ch/user/l/lumipro/public/Normtags/normtag_PHYSICS.json", "v1"),
      },
      "pu": {
        "json": (f"https://cms-service-dqmdc.web.cern.ch/CAF/certification/Collisions22/PileUp/BCDEFG/pileup_JSON.txt", "v1"),  # noqa
        "mc_profile": ("https://raw.githubusercontent.com/cms-sw/cmssw/bb525104a7ddb93685f8ced6fed1ab793b2d2103/SimGeneral/MixingModule/python/Run3_2022_LHC_Simulation_10h_2h_cfi.py", "v1"),  # noqa
        "data_profile": {
          "nominal": (f"/afs/cern.ch/user/a/anhaddad/public/Collisions22/pileupHistogram-Cert_Collisions2022_355100_362760_GoldenJson-13p6TeV-69200ub-100bins.root", "v1"),  # noqa
          "minbias_xs_up": (f"/afs/cern.ch/user/a/anhaddad/public/Collisions22/pileupHistogram-Cert_Collisions2022_355100_362760_GoldenJson-13p6TeV-72400ub-100bins.root", "v1"),  # noqa
          "minbias_xs_down": (f"/afs/cern.ch/user/a/anhaddad/public/Collisions22/pileupHistogram-Cert_Collisions2022_355100_362760_GoldenJson-13p6TeV-66000ub-100bins.root", "v1"),  # noqa
        },
      },
    }))
  else:
    raise NotImplementedError(f"No lumi and pu files provided for year {year}")

  # columns to keep after certain steps
  cfg.x.keep_columns = DotDict.wrap({
    "cf.SelectEvents": {"mc_weight"},
    "cf.MergeSelectionMasks": {
      "mc_weight", "normalization_weight", "process_id", "category_ids", "cutflow.*",
    },
  })

  cfg.x.keep_columns["cf.ReduceEvents"] = (
    {
      # general event information
      "run", "luminosityBlock", "event", "cutflow.*",
      # columns added during selection, required in general
      "mc_weight", "normalization_weight", "PV.npvs", "process_id", "category_ids", "deterministic_seed",
      # weight-related columns used by histogramming/default.py:all_weights
      # keep lepton SF weights explicitly; all_weights declares them in cfg.x.event_weights below
      "electron_weight*", "electron_mid_weight*", "electron_id_weight*",
      "muon_id_weight*", "muon_iso_weight*",
      "pu_weight*", "pdf_weight*",
      "murmuf_envelope_weight*", "mur_weight*", "muf_weight*",
      "btag_weight*",
      # needed by cms theory-weight producers used in xyh/production/default.py
      "LHEPdfWeight", "LHEScaleWeight",
      "Pileup.nTrueInt",
      "GenPart.*",
      # shifted JEC/JER columns produced during calibration
      "Jet.pt_*", "Jet.mass_*",
      "MET.pt_*", "MET.phi_*",
    } | set(  # Jets
      f"{jet_obj}.{field}"
      for jet_obj in ["Jet"]
      # NOTE: if we run into storage troubles, skip Bjet and Lightjet
      for field in ["pt", "eta", "phi", "mass", "genJetIdx", "btagDeepFlavB", "hadronFlavour", "rawFactor"]
    ) | set(  # BJets
      f"{jet_obj}.{field}"
      for jet_obj in ["Bjet"]
      # NOTE: if we run into storage troubles, skip Bjet and Lightjet
      for field in ["pt", "eta", "phi", "mass", "btagDeepFlavB", "hadronFlavour"]
    ) | set(  # Lightjets
      f"{jet_obj}.{field}"
      for jet_obj in ["Lightjet"]
      # NOTE: if we run into storage troubles, skip Bjet and Lightjet
      for field in ["pt", "eta", "phi", "mass", "genJetIdx", "btagDeepFlavB", "hadronFlavour", "rawFactor"]
    ) | set(  # Muons
      f"{mu_obj}.{field}"
      for mu_obj in ["Muon"]
      # NOTE: if we run into storage troubles, skip Bjet and Lightjet
      for field in ["pt", "eta", "phi", "mass", "pdgId"]
    ) | set(  # Electrons
      f"{e_obj}.{field}"
      for e_obj in ["Electron"]
      # NOTE: if we run into storage troubles, skip Bjet and Lightjet
      for field in ["pt", "eta", "phi", "mass", "pdgId", "deltaEtaSC"]
    ) | set(  # MET
      f"MET.{field}"
      for field in ["pt", "phi"]
    ) | set(  # MET
      f"GenMET.{field}"
      for field in ["pt", "phi"]
    ) | set(  # GenJets
      f"{gen_jet_obj}.{field}"
      for gen_jet_obj in ["GenJet"]
      for field in ["pt", "eta", "phi", "mass","hadronFlavour"]
    )
  )

  # event weight columns as keys in an ordered dict, mapped to shift instances they depend on
  # get_shifts = lambda *keys: sum(([cfg.get_shift(f"{k}_up"), cfg.get_shift(f"{k}_down")] for k in keys), [])
  # get_shifts = functools.partial(get_shifts_from_sources, cfg)
  cfg.x.event_weights = DotDict({
    # consumed by default hist producer "all_weights"
    "normalization_weight": [],
    "electron_weight": get_shifts_from_sources(cfg, "e_sf"),
    "electron_mid_weight": get_shifts_from_sources(cfg, "e_sf"),
    "electron_id_weight": get_shifts_from_sources(cfg, "e_sf"),
    "muon_id_weight": get_shifts_from_sources(cfg, "muon"),
    "muon_iso_weight": get_shifts_from_sources(cfg, "muon"),
    "pu_weight": get_shifts_from_sources(cfg, "minbias_xs"),
    "pdf_weight": get_shifts_from_sources(cfg, "pdf"),
    # "mur_weight": get_shifts_from_sources(cfg, "mur"),
    # "muf_weight": get_shifts_from_sources(cfg, "muf"),
    "murmuf_envelope_weight": get_shifts_from_sources(cfg, "murf_envelope"),
    # propagate b-tag shape shifts to histogram production
    "btag_weight": get_shifts_from_sources(
      cfg,
      "btag_cferr1",
      "btag_cferr2",
      "btag_hf",
      "btag_hfstats1",
      "btag_hfstats2",
      "btag_lf",
      "btag_lfstats1",
      "btag_lfstats2",
      "jec_Total",
    ),
  })

  for dataset in cfg.datasets:
    if dataset.x("is_ttbar", False):
      dataset.x.event_weights = {"top_pt_weight": get_shifts_from_sources(cfg, "top_pt")}

  prod_version = "v1"

  # Version of required tasks
  cfg.x.versions = {
      "cf.CalibrateEvents": prod_version, # "v0",
      "cf.SelectEvents": prod_version,
      "cf.MergeSelectionStats": prod_version,
      "cf.MergeSelectionMasks": prod_version,
      "cf.ReduceEvents": prod_version,
      "cf.MergeReductionStats": prod_version,
      "cf.MergeReduceEvents": prod_version,
      "cf.ProvideReducedEvents": prod_version,
      "cf.ProduceColumns": prod_version,
  }

  # add categories

  add_variables(cfg)
  add_all_categories(cfg)

  # TODO: Define and add triggers
  # if year == 2022:
  #   from xyh.config.triggers import add_triggers_2022
  #   add_triggers_2022(cfg)

  # only produce cutflow features when number of dataset_files is limited (used in selection module)
  cfg.x.do_cutflow_features = bool(limit_dataset_files) and limit_dataset_files <= 10

  # custom plotting style groups used throughout the analysis
  cfg.x.custom_style_config_groups = {
      "default": {
          "legend_cfg": {
              "ncols": 2,
              "fontsize": 16,
              "bbox_to_anchor": (0.0, 0.0, 1.0, 1.0),
              "loc": "upper center",
          },
          "annotate_cfg": {
              "xy": (0.05, 0.95),
              "xycoords": "axes fraction",
              "fontsize": 16,
          },
      },
      "legend_single_col": {
          "legend_cfg": {"ncols": 1, "fontsize": 20},
      },
      "small_legend": {
          "legend_cfg": {"ncols": 2, "fontsize": 16},
      },
      "no_cat_label": {
          "legend_cfg": {"ncols": 2, "fontsize": 20},
          "annotate_cfg": {"text": ""},
      },
      "signals": {
          "legend_cfg": {"ncols": 2, "fontsize": 12},
      },
      "example": {
          "legend_cfg": {"title": "my custom legend title", "ncols": 2},
          "ax_cfg": {"ylabel": "my ylabel", "xlim": (0, 100)},
          "rax_cfg": {"ylabel": "some other ylabel"},
          "annotate_cfg": {"text": "category label usually here"},
      },
  }
  return cfg
