# coding: utf-8

"""
Inference model that defines the datacard structure for the XYH analysis.

The model exposes a single signal process whose name is taken from the
``XYH_SIGNAL_PROCESS`` environment variable (e.g. ``xyh_sl_x1000_y350``).  This
keeps the inference description flexible enough to be reused for all signal
mass hypotheses while still producing signal process names that follow the
conventional naming scheme.
"""

from __future__ import annotations

import os
import re
from typing import Iterable

from columnflow.inference import (
    inference_model,
    ParameterType,
    ParameterTransformation,
)

from .signals import XYH_SIGNAL_PROCESSES


# categories entering the statistical model: (datacard name, config category)
CATEGORY_SPECS: tuple[tuple[str, str], ...] = (
    # ("cat_incl", "cat_incl"),
    # exclusive categories
    # categories dominated by background are dropped for limits
    # ("cat_1e_2bjets_4jets", "1e__2bjets__4jets"),
    # ("cat_1e_2bjets_5jets", "1e__2bjets__5jets"),
    # ("cat_1e_2bjets_6jets", "1e__2bjets__6jets"),
    # ("cat_1e_2bjets_g6jets", "1e__2bjets__g6jets"),
    # ("cat_1e_3bjets_4jets", "1e__3bjets__4jets"),
    # ("cat_1e_3bjets_5jets", "1e__3bjets__5jets"),
    # ("cat_1e_3bjets_6jets", "1e__3bjets__6jets"),
    # ("cat_1e_3bjets_g6jets", "1e__3bjets__g6jets"),
    # ("cat_1e_4bjets_5jets", "1e__4bjets__5jets"),
    # ("cat_1e_4bjets_6jets", "1e__ge4bjets__ge6jets"),

    # ("cat_1lep_2bjets_4jets", "1lep__2bjets__4jets"),
    # ("cat_1lep_2bjets_5jets", "1lep__2bjets__5jets"),
    # ("cat_1lep_2bjets_6jets", "1lep__2bjets__6jets"),
    # ("cat_1lep_2bjets_g6jets", "1lep__2bjets__g6jets"),
    ("cat_1lep_3bjets_4jets", "1lep__3bjets__4jets"),
    ("cat_1lep_3bjets_5jets", "1lep__3bjets__5jets"),
    # ("cat_1lep_3bjets_6jets", "1lep__3bjets__6jets"),
    # ("cat_1lep_3bjets_g6jets", "1lep__3bjets__g6jets"),
    ("cat_1lep_3bjets_ge6jets", "1lep__3bjets__ge6jets"),
    ("cat_1lep_4bjets_5jets", "1lep__4bjets__5jets"),
    ("cat_1lep_ge4bjets_ge6jets", "1lep__ge4bjets__ge6jets"),

    # inclusive examples (kept for reference)
    # ("cat_1e_ge2bjets_ge4jets", "1e__ge2bjets__ge4jets"),
    # ("cat_1e_ge2bjets_ge5jets", "1e__ge2bjets__ge5jets"),
    # ("cat_1e_ge3bjets_ge4jets", "1e__ge3bjets__ge4jets"),
    # ("cat_1e_ge3bjets_ge5jets", "1e__ge3bjets__ge5jets"),
    # ("cat_1e_ge3bjets_ge6jets", "1e__ge3bjets__ge6jets"),
    # ("cat_1e_ge4bjets_ge5jets", "1e__ge4bjets__ge5jets"),
    # ("cat_1e_ge4bjets_ge6jets", "1e__ge4bjets__ge6jets"),
    # ("cat_1mu_ge2bjets_ge4jets", "1mu__ge2bjets__ge4jets"),
    # ("cat_1mu_ge2bjets_ge5jets", "1mu__ge2bjets__ge5jets"),
    # ("cat_1mu_ge3bjets_ge4jets", "1mu__ge3bjets__ge4jets"),
    # ("cat_1mu_ge3bjets_ge5jets", "1mu__ge3bjets__ge5jets"),
    # ("cat_1mu_ge3bjets_ge6jets", "1mu__ge3bjets__ge6jets"),
    # ("cat_1mu_ge4bjets_ge5jets", "1mu__ge4bjets__ge5jets"),
    # ("cat_1mu_ge4bjets_ge6jets", "1mu__ge4bjets__ge6jets"),
)



# background process specifications (process label, config process name, dataset names)
BACKGROUND_SPECS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("TT", "tt", ("tt_sl_powheg", "tt_dl_powheg", "tt_fh_powheg")),
    ("DY", "dy", (
        "dy_m4to10_amcatnlo",
        "dy_m10to50_amcatnlo",
        #"dy_m50toinf_amcatnlo",
        "dy_m50toinf_0j_amcatnlo",
        "dy_m50toinf_1j_amcatnlo",
        "dy_m50toinf_2j_amcatnlo",
    )),
    ("ST", "st", (
        "st_tchannel_t_4f_powheg",
        "st_tchannel_tbar_4f_powheg",
        "st_twchannel_t_sl_powheg",
        "st_twchannel_t_dl_powheg",
        "st_twchannel_t_fh_powheg",
        "st_twchannel_tbar_sl_powheg",
        "st_twchannel_tbar_dl_powheg",
        "st_twchannel_tbar_fh_powheg",
    )),
    ("WJets", "w_lnu", ("w_lnu_amcatnlo",)),
    ("TTV", "ttz", ("ttz_zqq_1j_amcatnlo",)),
    ("TTWW", "ttww", ("ttww_madgraph",)),
    ("TTZZ", "ttzz", ("ttzz_madgraph",)),
    ("WW", "ww", ("ww_pythia",)),
    ("WZ", "wz", ("wz_pythia",)),
    ("ZZ", "zz", ("zz_pythia",)),
)

# parameter names controlling individual background normalisations
BACKGROUND_RATE_PARAMETERS: tuple[str, ...] = (
    "tt_norm",
    "dy_norm",
    "st_norm",
    "wjets_norm",
    "ttv_norm",
    "ttvv_norm",
    "vv_norm",
)

# shift sources used for b-tagging shape uncertainties
BTAG_SHAPE_SOURCES: tuple[str, ...] = (
    "btag_cferr1",
    "btag_cferr2",
    "btag_hf",
    "btag_hfstats1",
    "btag_hfstats2",
    "btag_lf",
    "btag_lfstats1",
    "btag_lfstats2",
)

def _resolve_signal_process() -> tuple[str, str, str]:
    """
    Returns the signal process name, corresponding dataset, and the score variable, validating the
    environment variable.
    """
    signal_process = os.environ.get("XYH_SIGNAL_PROCESS", "")
    if not signal_process:
        raise RuntimeError(
            "XYH_SIGNAL_PROCESS environment variable is not set. "
            "Please export the signal process name (e.g. 'xyh_sl_x1000_y350') "
            "before invoking the datacard creation tasks.",
        )

    if signal_process not in XYH_SIGNAL_PROCESSES:
        raise ValueError(
            f"XYH signal process '{signal_process}' is not known. "
            "Make sure the name matches one of the entries in XYH_SIGNAL_PROCESSES.",
        )

    match = re.match(r"xyh_sl_x(?P<x>\d+)_y(?P<y>\d+)", signal_process)
    if not match:
        raise ValueError(f"failed to extract masses from signal process '{signal_process}'")

    mass_x = match.group("x")
    mass_y = match.group("y")
    ml_model = f"xyh_binary_x{mass_x}_y{mass_y}"

    variable_template = os.environ.get("XYH_DATACARD_VARIABLE")
    if variable_template:
        try:
            score_variable = variable_template.format(
                signal=signal_process,
                ml_model=ml_model,
                mass_x=mass_x,
                mass_y=mass_y,
            )
        except KeyError as exc:
            raise ValueError(
                f"failed to build datacard variable from template '{variable_template}': missing key {exc}",
            ) from exc
    else:
        score_variable = f"nn_score__{ml_model}"

    dataset = f"{signal_process}_madgraph"
    return signal_process, dataset, score_variable


def _background_process_names() -> Iterable[str]:
    for proc, _, _ in BACKGROUND_SPECS:
        yield proc


@inference_model
def xyh_limits(self) -> None:
    """
    Datacard layout for the XYH single-lepton analysis.
    """
    signal_process, signal_dataset, score_variable = _resolve_signal_process()

    #
    # categories
    #

    for cat_name, config_category in CATEGORY_SPECS:
        category_config_data = {
            config_inst.name: self.category_config_spec(
                category=config_category,
                variable=score_variable,
            )
            for config_inst in self.config_insts
        }
        self.add_category(
            cat_name,
            config_data=category_config_data,
            data_from_processes=list(_background_process_names()),
            mc_stats=False,
        )

    #
    # processes
    #

    # background processes
    for name, config_process, datasets in BACKGROUND_SPECS:
        process_config_data = {
            config_inst.name: self.process_config_spec(
                process=config_process,
                mc_datasets=list(datasets),
            )
            for config_inst in self.config_insts
        }
        self.add_process(
            name,
            config_data=process_config_data,
        )

    # signal process
    signal_process_config_data = {
        config_inst.name: self.process_config_spec(
            process=signal_process,
            mc_datasets=[signal_dataset],
        )
        for config_inst in self.config_insts
    }
    self.add_process(
        signal_process,
        is_signal=True,
        config_data=signal_process_config_data,
    )

    #
    # parameters
    #
    # Experimental uncertainties: luminosity and b-tagging.
    self.add_parameter_group("experiment")

    for cfg in self.config_insts:
        lumi = cfg.x.luminosity
        for unc_name in ("lumi_13TeV_2022", "lumi_13TeV_correlated"):
            if unc_name not in lumi.uncertainties:
                continue

            self.add_parameter(
                f"{unc_name}_{cfg.name}",
                type=ParameterType.rate_gauss,
                effect=lumi.get(names=unc_name, direction=("down", "up"), factor=True),
                transformations=[ParameterTransformation.symmetrize],
                group="experiment",
            )

    all_mc_processes = [*list(_background_process_names()), signal_process]
    self.add_parameter(
        "minbias_xs",
        process=all_mc_processes,
        type=ParameterType.shape,
        config_data={
            cfg.name: self.parameter_config_spec(shift_source="minbias_xs")
            for cfg in self.config_insts
        },
        group="experiment",
    )

    # self.add_parameter(
    #     "top_pt",
    #     process="TT",
    #     type=ParameterType.shape,
    #     config_data={
    #         cfg.name: self.parameter_config_spec(shift_source="top_pt")
    #         for cfg in self.config_insts
    #     },
    #     group="experiment",
    # )

    #
    # b-tagging shape uncertainties
    #
    for shift_source in BTAG_SHAPE_SOURCES:
        for cfg in self.config_insts:
            if not cfg.has_shift(f"{shift_source}_up") or not cfg.has_shift(f"{shift_source}_down"):
                raise ValueError(
                    f"required shifts '{shift_source}_up/down' not found in config '{cfg.name}'",
                )

        self.add_parameter(
            shift_source,
            process=all_mc_processes,
            type=ParameterType.shape,
            config_data={
                cfg.name: self.parameter_config_spec(shift_source=shift_source)
                for cfg in self.config_insts
            },
            group="experiment",
        )

    for shift_source in (
        "e_sf",
        "e_trig_sf",
        "muon",
        # "jer",
    ):
        for cfg in self.config_insts:
            if not cfg.has_shift(f"{shift_source}_up") or not cfg.has_shift(f"{shift_source}_down"):
                raise ValueError(
                    f"required shifts '{shift_source}_up/down' not found in config '{cfg.name}'",
                )

        self.add_parameter(
            shift_source,
            process=all_mc_processes,
            type=ParameterType.shape,
            config_data={
                cfg.name: self.parameter_config_spec(shift_source=shift_source)
                for cfg in self.config_insts
            },
            group="experiment",
        )

    #
    # free-floating TT normalisation
    #

    if not self.has_parameter_group("theory"):
        self.add_parameter_group("theory")
    self.add_parameter(
        "tt_norm",
        process="TT",
        type=ParameterType.rate_unconstrained,
        effect=os.environ.get("XYH_TT_NORM_RATEPARAM", "1 [0,2]"),
        group="theory",
    )

    # self.add_parameter_group("theory")
    #
    # flat normalisation uncertainties for backgrounds
    # for param_name, (proc_name, _, _) in zip(BACKGROUND_RATE_PARAMETERS, BACKGROUND_SPECS):
    #     if param_name == "tt_norm":
    #         continue
    #     self.add_parameter(
    #         param_name,
    #         process=proc_name,
    #         type=ParameterType.rate_uniform,
    #         group="theory",
    #     )

    for shift_source in (
        # "mur",
        # "muf",
        "murf_envelope",
        "pdf",
    ):
        for cfg in self.config_insts:
            if not cfg.has_shift(f"{shift_source}_up") or not cfg.has_shift(f"{shift_source}_down"):
                raise ValueError(
                    f"required shifts '{shift_source}_up/down' not found in config '{cfg.name}'",
                )

        self.add_parameter(
            shift_source,
            process=all_mc_processes,
            type=ParameterType.shape,
            config_data={
                cfg.name: self.parameter_config_spec(shift_source=shift_source)
                for cfg in self.config_insts
            },
            group="theory",
        )

    # for shift_source in ("tune", "hdamp"):
    #     for cfg in self.config_insts:
    #         if not cfg.has_shift(f"{shift_source}_up") or not cfg.has_shift(f"{shift_source}_down"):
    #             raise ValueError(
    #                 f"required shifts '{shift_source}_up/down' not found in config '{cfg.name}'",
    #             )

    #     self.add_parameter(
    #         shift_source,
    #         process="TT",
    #         type=ParameterType.shape,
    #         config_data={
    #             cfg.name: self.parameter_config_spec(shift_source=shift_source)
    #             for cfg in self.config_insts
    #         },
    #         group="theory",
    #     )
    
    # signal strength modifier
    # self.add_parameter(
    #     "mu_signal",
    #     process=signal_process,
    #     type=ParameterType.rate_uniform,
    #     group="theory",
    # )

# Ensure downstream tasks know data handling policy
xyh_limits.skip_data = False
xyh_limits.unblind = False
