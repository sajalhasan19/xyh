
# coding: utf-8

"""
Collection of patches of underlying columnflow tasks.
"""

import os

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
def patch_all():
    patch_bundle_repo_exclude_files()
    patch_plot_ml_results_single_config()
