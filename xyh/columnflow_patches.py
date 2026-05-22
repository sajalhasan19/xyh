
# coding: utf-8

"""
Collection of patches of underlying columnflow tasks.
"""

import os
import shlex

import law
from columnflow.util import memoize


logger = law.logger.get_logger(__name__)


@memoize
def patch_bundle_repo_exclude_files():
    from columnflow.tasks.framework.remote import BundleRepo

    # get the relative path to CF_BASE
    cf_rel = os.path.relpath(os.environ["CF_BASE"], os.environ["XYH_BASE"])

    # amend exclude files to start with the relative path to CF_BASE
    exclude_files = [os.path.join(cf_rel, path) for path in BundleRepo.exclude_files]

    # add additional files
    exclude_files.extend([
        "docs", "tests", "data", "assets", ".law", ".setups", ".data", ".github",
    ])

    # overwrite them
    BundleRepo.exclude_files[:] = exclude_files

    logger.debug("patched exclude_files of cf.BundleRepo")


@memoize
def patch_plot_ml_results_single_config():
    try:
        from columnflow.tasks.ml import PlotMLResults, MergeMLEvaluation
    except Exception as exc:
        logger.warning("failed to import PlotMLResults for patching: %s", exc)
        return

    if getattr(PlotMLResults, "single_config", None) is True:
        if getattr(PlotMLResults, "resolution_task_cls", None) is MergeMLEvaluation:
            return
    else:
        PlotMLResults.single_config = True
        if hasattr(PlotMLResults, "__abstractmethods__") and "single_config" in PlotMLResults.__abstractmethods__:
            PlotMLResults.__abstractmethods__ = frozenset(
                m for m in PlotMLResults.__abstractmethods__ if m != "single_config"
            )
        logger.debug("patched PlotMLResults.single_config -> True")

    if getattr(PlotMLResults, "resolution_task_cls", None) is not MergeMLEvaluation:
        PlotMLResults.resolution_task_cls = MergeMLEvaluation
        logger.debug("patched PlotMLResults.resolution_task_cls -> MergeMLEvaluation")


@memoize
def patch_remote_forward_xyh_env():
    from columnflow.tasks.framework.remote import HTCondorWorkflow, SlurmWorkflow

    env_names = (
        "ML_SETTINGS",
        "XYH_PNN_SETTINGS",
        "COND_MASSES",
        "EVAL_MASS_X",
        "EVAL_MASS_Y",
        "XYH_SIGNAL_PROCESS",
        "XYH_DATACARD_VARIABLE",
    )

    def build_export_command() -> str:
        return "\n".join(
            f"export {name}={shlex.quote(os.environ[name])}"
            for name in env_names
            if name in os.environ
        )

    def append_post_setup_exports(config) -> None:
        command = build_export_command()
        if not command:
            return

        current = config.render_variables.get("cf_post_setup_command", "")
        config.render_variables["cf_post_setup_command"] = "\n".join(
            part for part in (current, command) if part
        )

    if not getattr(HTCondorWorkflow.htcondor_job_config, "_xyh_env_patched", False):
        original_htcondor_job_config = HTCondorWorkflow.htcondor_job_config

        def htcondor_job_config(self, config, job_num, branches):
            config = original_htcondor_job_config(self, config, job_num, branches)
            append_post_setup_exports(config)
            return config

        htcondor_job_config._xyh_env_patched = True
        HTCondorWorkflow.htcondor_job_config = htcondor_job_config

    if not getattr(SlurmWorkflow.slurm_job_config, "_xyh_env_patched", False):
        original_slurm_job_config = SlurmWorkflow.slurm_job_config

        def slurm_job_config(self, config, job_num, branches):
            config = original_slurm_job_config(self, config, job_num, branches)
            append_post_setup_exports(config)
            return config

        slurm_job_config._xyh_env_patched = True
        SlurmWorkflow.slurm_job_config = slurm_job_config

    logger.debug("patched remote workflows to export XYH environment variables")


@memoize
def patch_all():
    patch_bundle_repo_exclude_files()
    patch_plot_ml_results_single_config()
    patch_remote_forward_xyh_env()
