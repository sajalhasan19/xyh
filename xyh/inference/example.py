# coding: utf-8

"""
Example inference model.
"""

from columnflow.inference import inference_model, ParameterType, ParameterTransformation

from .signals import XYH_SIGNAL_DATASETS


@inference_model
def example(self):

    #
    # categories
    #

    self.add_category(
        "cat1",
        config_category="incl",
        config_variable="jet1_pt",
        config_data_datasets=["data_mu_b"],
        mc_stats=True,
    )
    self.add_category(
        "cat2",
        config_category="2j",
        config_variable="jet1_eta",
        # fake data from TT
        data_from_processes=["TT"],
        mc_stats=True,
    )
    self.add_category(
        "cat3",
        config_category="2j",
        config_variable="jet1_eta",
        data_from_processes=["ST"],
        mc_stats=True,
    )
    self.add_category(
        "cat_ttvv_test",
        config_category="incl",
        config_variable="jet1_pt",
        data_from_processes=["TTVV"],
        mc_stats=True,
    )
    self.add_category(
        "cat_ttv_test",
        config_category="incl",
        config_variable="jet1_pt",
        data_from_processes=["TTV"],
        mc_stats=True,
    )
    self.add_category(
        "xyh_signal_incl",
        config_category="incl",  # or "2j", "3b", etc., as defined in your config
        config_variable="jet1_pt",
        data_from_processes=["XYH"],  # this is the name you gave in add_process
        mc_stats=True,
    )



    #
    # processes
    #

    self.add_process(
        "ST",
        is_signal=True,
        config_process="st",
        config_mc_datasets=["st_tchannel_t_4f_powheg"],
    )
    self.add_process(
        "TT",
        config_process="tt",
        config_mc_datasets=["tt_sl_powheg"],
    )

    self.add_process(
        "TTVV",
        config_process="ttvv",
        config_mc_datasets=[
            "ttzz_madgraph",
            "ttww_madgraph",
            # whatever ttVV samples you have
        ],
    )

    self.add_process(
        "XYH",
        is_signal=True,
        config_process="xyh",  # must match procs.xyh
        config_mc_datasets=list(XYH_SIGNAL_DATASETS),  # must match dataset name in campaign
    )

    self.add_process(
        "TTV",
        config_process="ttv",
        config_mc_datasets=[
            "ttz_amcatnlo"
            # whatever ttV samples you have
        ],
    )

    #
    # parameters
    #

    # groups
    self.add_parameter_group("experiment")
    self.add_parameter_group("theory")

    # lumi
    lumi = self.config_inst.x.luminosity
    for unc_name in lumi.uncertainties:
        self.add_parameter(
            unc_name,
            type=ParameterType.rate_gauss,
            effect=lumi.get(names=unc_name, direction=("down", "up"), factor=True),
            transformations=[ParameterTransformation.symmetrize],
        )

    # tune uncertainty
    self.add_parameter(
        "tune",
        process="TT",
        type=ParameterType.shape,
        config_shift_source="tune",
    )

    # muon weight uncertainty
    self.add_parameter(
        "mu",
        process=["ST", "TT"],
        type=ParameterType.shape,
        config_shift_source="mu",
    )

    # jet energy correction uncertainty
    self.add_parameter(
        "jec",
        process=["ST", "TT"],
        type=ParameterType.shape,
        config_shift_source="jec",
    )

    # a custom asymmetric uncertainty that is converted from rate to shape
    self.add_parameter(
        "QCDscale_ttbar",
        process="TT",
        type=ParameterType.shape,
        transformations=[ParameterTransformation.effect_from_rate],
        effect=(0.5, 1.1),
    )
    
    self.add_parameter(
        "ttvv_norm",
        process="TTVV",
        type=ParameterType.rate_flat,
    )

    self.add_parameter(
        "ttv_norm",
        process="TTV",
        type=ParameterType.rate_flat,
    )
    
    self.add_parameter(
        "mu_signal",
        process="XYH",
        type=ParameterType.rate_flat,
        group="theory",
    )




@inference_model
def example_no_shapes(self):
    # same initialization as "example" above
    example.init_func.__get__(self, self.__class__)()

    #
    # remove all shape parameters
    #

    for category_name, process_name, parameter in self.iter_parameters():
        if parameter.type.is_shape or any(trafo.from_shape for trafo in parameter.transformations):
            self.remove_parameter(parameter.name, process=process_name, category=category_name)
