# coding: utf-8

"""
Configuration of the xyh analysis.
"""

import law
import order as od
from scinum import Number

from columnflow.config_util import (
    get_root_processes_from_campaign, add_shift_aliases, add_category, verify_config_processes,
)
from columnflow.columnar_util import EMPTY_FLOAT, ColumnCollection, skip_column
from columnflow.util import DotDict, maybe_import

ak = maybe_import("awkward")


#
# the main analysis object
#

analysis_xyh = ana = od.Analysis(
    name="analysis_xyh",
    id=1,
)

# analysis-global versions
# (see cfg.x.versions below for more info)
ana.x.versions = {}

# files of bash sandboxes that might be required by remote tasks
# (used in cf.HTCondorWorkflow)
ana.x.bash_sandboxes = ["$CF_BASE/sandboxes/cf.sh"]
default_sandbox = law.Sandbox.new(law.config.get("analysis", "default_columnar_sandbox"))
if default_sandbox.sandbox_type == "bash" and default_sandbox.name not in ana.x.bash_sandboxes:
    ana.x.bash_sandboxes.append(default_sandbox.name)

# files of cmssw sandboxes that might be required by remote tasks
# (used in cf.HTCondorWorkflow)
ana.x.cmssw_sandboxes = [
    "$CF_BASE/sandboxes/cmssw_default.sh",
]

# config groups for conveniently looping over certain configs
# (used in wrapper_factory)
ana.x.config_groups = {}

# named function hooks that can modify store_parts of task outputs if needed
ana.x.store_parts_modifiers = {}


#
# setup configs
#

# an example config is setup below, based on cms NanoAOD v9 for Run2 2017, focussing on
# ttbar and single top MCs, plus single muon data
# update this config or add additional ones to accomodate the needs of your analysis

from xyh.config.config_run3 import add_config
# from cmsdb.campaigns.run2_2017_nano_v9 import campaign_run2_2017_nano_v9
import cmsdb.campaigns.run3_2022_preEE_nano_v12

# copy the campaign
# (creates copies of all linked datasets, processes, etc. to allow for encapsulated customization)
# campaign = campaign_run2_2017_nano_v9.copy()
campaign_run3_2022_preEE_nano_v12 = cmsdb.campaigns.run3_2022_preEE_nano_v12.campaign_run3_2022_preEE_nano_v12
campaign_run3_2022_preEE_nano_v12.x.EE = "pre"

config_2022pre = add_config(
  analysis_xyh,
  campaign_run3_2022_preEE_nano_v12.copy(),
  config_name="config_2022pre",
  config_id=1,
)
config_2022pre_limited = add_config(
  analysis_xyh,
  campaign_run3_2022_preEE_nano_v12.copy(),
  config_name="config_2022pre_limited",
  config_id=12,
  limit_dataset_files=1,
)
