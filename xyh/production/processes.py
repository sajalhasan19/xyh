# coding: utf-8

"""
Custom process assignment for XYH.
"""

from __future__ import annotations

import law
import order as od

from columnflow.columnar_util import set_ak_column
from columnflow.production import Producer, producer
from columnflow.production.processes import process_ids
from columnflow.util import maybe_import

np = maybe_import("numpy")
ak = maybe_import("awkward")

logger = law.logger.get_logger(__name__)


def get_process_id_from_masks(
    events: ak.Array,
    process_masks: dict[str, ak.Array],
    dataset_inst: od.Dataset,
) -> ak.Array:
    """
    Assign a process id per event based on boolean masks.
    """
    leaf_procs = dataset_inst.get_leaf_processes()

    process_id = ak.Array(np.zeros(len(events), dtype=np.int32))
    for proc_name, mask in process_masks.items():
        if not ak.any(mask):
            continue

        if not dataset_inst.has_process(proc_name):
            raise NotImplementedError(
                f"events from dataset {dataset_inst.name} are assigned process {proc_name} "
                f"but dataset has only {leaf_procs} registered as leaf processes",
            )

        proc_id = dataset_inst.get_process(proc_name).id
        if not ak.all(process_id[mask] == 0):
            raise ValueError(f"events from dataset {dataset_inst.name} have overlapping processes")

        process_id = ak.where(mask, proc_id, process_id)

    if ak.any(process_id == 0):
        raise ValueError(f"events from dataset {dataset_inst.name} have not been assigned any process")

    return process_id


@producer(
    uses={"genTtbarId"},
    produces={"process_id"},
    mc_only=True,
)
def tt_process_producer(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Split tt/ttbb powheg samples into subprocesses using ``genTtbarId``.

    For inclusive ``tt_*`` samples, keep the split exclusive with respect to the
    dedicated ``ttbb_*`` samples:

    - ``*_1b`` only receives ``genTtbarId == 51`` events,
    - ``*_nonb`` receives the light- and charm-flavour contribution,
    - ``ttbb``-like events in the inclusive ``tt_*`` samples are routed to
      ``*_hf`` support processes so they can be inspected separately from the
      inclusive ``tt`` parent and excluded when the analysis uses the dedicated
      ``ttbb`` samples.

    Dedicated ``ttbb_*`` samples keep their internal ``1b`` / ``nonb`` split.
    """
    top_id = events.genTtbarId % 100
    is_ttb = top_id == 51
    is_tt2b = top_id == 52
    is_tt_bb = (top_id >= 53) & (top_id <= 55)
    is_ttcc = (top_id >= 41) & (top_id <= 45)
    is_ttlf = top_id == 0

    is_ttbb = is_ttb | is_tt2b | is_tt_bb
    is_tt = is_ttcc | is_ttlf

    base_proc_name = "_".join(self.dataset_inst.name.split("_")[:2])
    if not base_proc_name.startswith("tt"):
        raise NotImplementedError(
            f"process producer {self.cls_name} for dataset {self.dataset_inst.name} is not implemented",
        )

    if base_proc_name.startswith("ttbb"):
        process_masks = {
            f"{base_proc_name}_1b": is_ttbb,
            f"{base_proc_name}_nonb": is_tt,
        }
    else:
        process_masks = {
            f"{base_proc_name}_hf": is_tt2b | is_tt_bb,
            f"{base_proc_name}_1b": is_ttb,
            f"{base_proc_name}_nonb": is_tt,
        }

    process_id = get_process_id_from_masks(events, process_masks, self.dataset_inst)
    return set_ak_column(events, "process_id", process_id, value_type=np.int32)


@producer(produces={"process_id"})
def xyh_process_ids(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Dispatch to a dataset-specific process producer when needed.
    """
    logger.info_once(
        f"{id(self)}_xyh_process_ids",
        f"running process producer {self.process_producer.cls_name} for dataset {self.dataset_inst.name}",
    )
    return self[self.process_producer](events, **kwargs)


@xyh_process_ids.init
def xyh_process_ids_init(self: Producer) -> None:
    if not hasattr(self, "dataset_inst"):
        return

    if self.dataset_inst.name.startswith(("tt_", "ttbb_")) and self.dataset_inst.name.endswith("_powheg"):
        self.process_producer = tt_process_producer
    elif len(self.dataset_inst.processes) == 1:
        self.process_producer = process_ids
    else:
        raise NotImplementedError(
            f"no process producer implemented for dataset {self.dataset_inst.name} "
            f"with processes {self.dataset_inst.processes.names()}",
        )

    self.uses.add(self.process_producer)
    self.produces.add(self.process_producer)
