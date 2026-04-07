# coding: utf-8

"""
PNN-style parameterized XYH classifier.
"""

from __future__ import annotations

import os
import pickle
import re
from collections import OrderedDict
from typing import Any, Iterable

import law
import numpy as np
import order as od

from columnflow.columnar_util import Route, set_ak_column
from columnflow.ml import MLModel
from columnflow.tasks.framework.parameters import SettingsParameter
from columnflow.util import dev_sandbox, maybe_import, maybe_int

from xyh.inference.signals import XYH_SIGNAL_PROCESSES

ak = maybe_import("awkward")
tf = maybe_import("tensorflow")

law.contrib.load("tensorflow")
logger = law.logger.get_logger(__name__)


class XYHPNNModel(MLModel):
    """PNN-style model adapted to XYH parameterized training."""

    single_config = False
    folds = 4
    require_mass_hypothesis = False

    default_parameters = OrderedDict([
        ("hidden_units", (512, 512, 512)),
        ("dropout", 0.3),
        ("learning_rate", 5.0e-4),
        ("l2_reg", 1.0e-5),
        ("batch_size", 500),
        ("epochs", 30),
        ("patience", 8),
        ("validation_split", 0.2),
        ("feature_set", "legacy"),
        ("reduce_lr_factor", 0.5),
        ("reduce_lr_patience", 3),
        ("early_stopping_min_delta", 1.0e-4),
        ("reduce_lr_min_delta", 1.0e-4),
        ("eqweight", True),
        ("conditioning_masses", None),
        ("background_mass_mode", "random"),
        ("background_mass_value", None),
        ("background_mass_seed", None),
        ("eval_mass_x", None),
        ("eval_mass_y", None),
        ("skip_missing_masses", True),
        ("require_conditioning_masses", True),
        ("training_categories", (
            "1lep__ge4bjets__ge6jets",
            "1lep__3bjets__g6jets",
            "1lep__3bjets__6jets",
            "1lep__3bjets__5jets",
            "1lep__4bjets__5jets",
        )),
    ])

    background_processes: tuple[str, ...] = (
        "dy",
        "tt",
        "ttz",
        "ttww",
        "ttzz",
        "ttz_zqq",
        "st",
        "w_lnu",
        "ww",
        "wz",
        "zz",
    )

    feature_routes: tuple[str, ...] = (
        "Jet.pt",
        "Jet.eta",
        "Jet.btagDeepFlavB",
        "Bjet.pt",
        "Bjet.eta",
        "Bjet.btagDeepFlavB",
        "Muon.pt",
        "Muon.eta",
        "Electron.pt",
        "Electron.eta",
        "MET.pt",
        "MET.phi",
        "m_X",
        "m_tt",
        "tt_pt",
        "top_pt",
        "top_mass",
        "m_lead_b",
        "lead_b_pt",
        "deltaR_jj",
        "deltaR_qq",
        "deltaR_bb",
        "whad_mass",
        "mlnu",
        "mtlnu",
        "wboson.pt",
        "tt_bar_mass",
        "tt_bar_pt",
        "whad_pt",
        "whad_eta",
        "whad_phi",
        "normalization_weight",
    )

    legacy_feature_names: tuple[str, ...] = (
        "ht",
        "n_jets",
        "jet1_pt",
        "jet2_pt",
        "jet1_eta",
        "jet2_eta",
        "jet1_btag",
        "jet2_btag",
        "muon1_pt",
        "electron1_pt",
        "muon1_eta",
        "electron1_eta",
        "n_muons",
        "n_electrons",
        "met_pt",
        "met_phi",
        "m_x",
        "m_tt",
        "tt_pt",
        "top_pt",
        "top_mass",
        "m_lead_b",
        "lead_b_pt",
        "delta_r_jj",
        "delta_r_qq",
        "delta_r_bb",
        "ht_additional",
        "total_btag",
        "n_additional_jets",
        "jet3_pt",
        "jet4_pt",
        "jet5_pt",
        "jet6_pt",
        "jet3_eta",
        "jet4_eta",
        "jet5_eta",
        "jet6_eta",
        "jet3_btag",
        "jet4_btag",
        "jet5_btag",
        "jet6_btag",
        "ht_bjets",
        "n_bjets",
        "bjet_sum_btag",
        "bjet1_pt",
        "bjet2_pt",
        "bjet3_pt",
        "bjet1_eta",
        "bjet2_eta",
        "bjet3_eta",
        "bjet1_btag",
        "bjet2_btag",
        "bjet3_btag",
    )

    curated_feature_names: tuple[str, ...] = (
        "ht",
        "n_jets",
        "n_bjets",
        "jet1_pt",
        "jet2_pt",
        "jet1_eta",
        "jet2_eta",
        "jet1_btag",
        "jet2_btag",
        "muon1_pt",
        "electron1_pt",
        "muon1_eta",
        "electron1_eta",
        "n_muons",
        "n_electrons",
        "met_pt",
        "met_phi",
        "m_x",
        "m_tt",
        "tt_pt",
        "top_pt",
        "top_mass",
        "m_lead_b",
        "lead_b_pt",
        "delta_r_bb",
        "delta_r_jj",
        "delta_r_qq",
        "ht_additional",
        "total_btag",
        "n_additional_jets",
        "ht_bjets",
        "bjet_sum_btag",
        "jet3_pt",
        "jet3_eta",
        "jet3_btag",
        "jet4_pt",
        "jet4_eta",
        "jet4_btag",
        "bjet1_pt",
        "bjet2_pt",
        "bjet3_pt",
        "bjet1_eta",
        "bjet2_eta",
        "bjet3_eta",
        "bjet1_btag",
        "bjet2_btag",
        "bjet3_btag",
    )

    feature_sets: dict[str, tuple[str, ...]] = {
        "legacy": legacy_feature_names,
        "all": legacy_feature_names,
        "curated": curated_feature_names,
    }

    def __init__(self, *args, **kwargs) -> None:
        requested_store_name = kwargs.get("store_name", law.no_value)
        super().__init__(*args, **kwargs)

        if ak is None or tf is None:
            raise RuntimeError("XYHPNNModel requires awkward and tensorflow to be available in the sandbox")

        env_settings = os.environ.get("ML_SETTINGS") or os.environ.get("XYH_PNN_SETTINGS")
        if env_settings:
            try:
                parsed = SettingsParameter().parse(env_settings)
            except Exception as exc:
                logger.warning("Failed to parse ML_SETTINGS for XYHPNNModel: %s", exc)
                parsed = {}
            for key, value in dict(parsed).items():
                if key not in self.parameters:
                    self.parameters[key] = value

        if self.parameters.get("conditioning_masses") in (None, "", ()):
            env_cond = os.environ.get("COND_MASSES")
            if env_cond:
                self.parameters["conditioning_masses"] = env_cond

        env_eval_x = os.environ.get("EVAL_MASS_X")
        env_eval_y = os.environ.get("EVAL_MASS_Y")
        if self.parameters.get("eval_mass_x") is None and env_eval_x:
            self.parameters["eval_mass_x"] = env_eval_x
        if self.parameters.get("eval_mass_y") is None and env_eval_y:
            self.parameters["eval_mass_y"] = env_eval_y

        for name, value in self.default_parameters.items():
            self.parameters.setdefault(name, value)

        # Keep ML data preparation/merge paths aligned with the training cache key.
        # This avoids creating new ML data stashes for eval-only mass changes.
        if requested_store_name in (law.no_value, None):
            self.store_name = self._training_model_repr()

    def setup(self) -> None:
        score_name = f"{self.cls_name}.score"
        if score_name not in self.config_inst.variables:
            self.config_inst.add_variable(
                name=score_name,
                null_value=-1.0,
                binning=(20, 0.0, 1.0),
                x_title=f"{self.cls_name} score",
            )

    def sandbox(self, task: law.Task) -> str:
        return dev_sandbox("bash::$XYH_BASE/sandboxes/example.sh")

    def _datasets_for_process(self, config_inst: od.Config, process_name: str) -> list[od.Dataset]:
        datasets: list[od.Dataset] = []

        try:
            process_inst = config_inst.get_process(process_name)
        except Exception:
            process_inst = None

        for dataset in getattr(config_inst, "datasets", []):
            ds_process = getattr(dataset, "process", None)
            if ds_process is None:
                if dataset.name == process_name or dataset.name.startswith(process_name + "_"):
                    datasets.append(dataset)
                continue
            if process_inst and ds_process is process_inst:
                datasets.append(dataset)
                continue
            if ds_process.name == process_name:
                datasets.append(dataset)
                continue
            has_parent = getattr(ds_process, "has_parent_process", None)
            if callable(has_parent) and has_parent(process_name):
                datasets.append(dataset)
                continue
            if dataset.name == process_name or dataset.name.startswith(process_name + "_"):
                datasets.append(dataset)

        return datasets

    def _available_mass_pairs(self) -> tuple[tuple[int, int], ...]:
        pairs = []
        for name in XYH_SIGNAL_PROCESSES:
            pair = self._normalize_mass_pair(name)
            if pair:
                pairs.append(pair)
        unique = []
        seen: set[tuple[int, int]] = set()
        for pair in pairs:
            if pair not in seen:
                unique.append(pair)
                seen.add(pair)
        return tuple(unique)

    def _normalize_mass_pair(self, raw: Any) -> tuple[int, int] | None:
        if isinstance(raw, (list, tuple)) and len(raw) == 2:
            try:
                return (int(float(raw[0])), int(float(raw[1])))
            except (TypeError, ValueError):
                return None
        if isinstance(raw, str):
            numbers = re.findall(r"-?\d+(?:\.\d+)?", raw)
            if len(numbers) >= 2:
                try:
                    return (int(float(numbers[0])), int(float(numbers[1])))
                except ValueError:
                    return None
        return None

    def _conditioning_masses(self) -> tuple[tuple[int, int], ...]:
        raw = self.parameters.get("conditioning_masses")
        if raw is None:
            if self.parameters.get("require_conditioning_masses", True):
                raise RuntimeError(
                    "conditioning_masses not provided; set --ml-model-settings "
                    "conditioning_masses=... to run the parameterized model."
                )
            return ()
        if isinstance(raw, str) and raw.strip().lower() == "all":
            candidates = self._available_mass_pairs()
        else:
            if isinstance(raw, str):
                raw_items: list[Any] = [part.strip() for part in raw.split(";") if part.strip()]
            elif isinstance(raw, (list, tuple)):
                raw_items = list(raw)
            else:
                raise ValueError(
                    f"conditioning_masses must be a string, list, or tuple (got {type(raw).__name__})"
                )
            masses = []
            for entry in raw_items:
                pair = self._normalize_mass_pair(entry)
                if pair is None:
                    raise ValueError(
                        f"Could not parse mass pair '{entry}' for conditioning_masses; "
                        "expected entries like '1200,100' or (1200, 100)."
                    )
                masses.append(pair)
            candidates = tuple(masses)

        if not candidates:
            raise RuntimeError("No signal mass points available for parameterized training.")

        if self.parameters.get("skip_missing_masses", True):
            candidate_set = set(candidates)
            available_in_data: set[tuple[int, int]] = set()
            for process_name in XYH_SIGNAL_PROCESSES:
                pair = self._normalize_mass_pair(process_name)
                if pair is None or pair not in candidate_set:
                    continue
                if self._datasets_for_process(self.config_inst, process_name):
                    available_in_data.add(pair)
            candidates = tuple(pair for pair in candidates if pair in available_in_data)
            if not candidates:
                raise RuntimeError(
                    "No signal masses with available datasets remain after filtering missing masses."
                )
        else:
            available = set(self._available_mass_pairs())
            missing = [pair for pair in candidates if pair not in available]
            if missing:
                available_str = ", ".join(f"x{mx}_y{my}" for mx, my in sorted(available))
                missing_str = ", ".join(f"x{mx}_y{my}" for mx, my in missing)
                raise RuntimeError(
                    f"conditioning_masses contains unavailable mass points: {missing_str}. "
                    f"Available: {available_str}"
                )

        ordered: list[tuple[int, int]] = []
        seen: set[tuple[int, int]] = set()
        for pair in candidates:
            if pair not in seen:
                ordered.append(pair)
                seen.add(pair)
        return tuple(ordered)

    def _signal_mass_map(self, cond_masses: tuple[tuple[int, int], ...]) -> dict[str, tuple[int, int]]:
        cond_set = set(cond_masses)
        found_masses: set[tuple[int, int]] = set()
        mass_map: dict[str, tuple[int, int]] = {}

        for process_name in XYH_SIGNAL_PROCESSES:
            pair = self._normalize_mass_pair(process_name)
            if pair is None or pair not in cond_set:
                continue
            datasets = self._datasets_for_process(self.config_inst, process_name)
            if datasets:
                found_masses.add(pair)
            for dataset in datasets:
                mass_map[dataset.name] = pair

        missing = cond_set - found_masses
        if missing:
            missing_str = ", ".join(f"x{mx}_y{my}" for mx, my in sorted(missing))
            if self.parameters.get("skip_missing_masses", True):
                logger.warning("Skipping missing signal masses in training: %s", missing_str)
            else:
                raise RuntimeError(f"No training datasets found for signal masses: {missing_str}")

        return mass_map

    def _background_mass_mode(self) -> str:
        mode = str(self.parameters.get("background_mass_mode", "duplicate")).strip().lower()
        if mode not in {"duplicate", "fixed", "random"}:
            raise ValueError("background_mass_mode must be 'duplicate', 'fixed', or 'random'")
        return mode

    def _resolve_eval_mass(self, cond_masses: tuple[tuple[int, int], ...]) -> tuple[int, int]:
        eval_mass_x = self.parameters.get("eval_mass_x")
        eval_mass_y = self.parameters.get("eval_mass_y")
        if eval_mass_x is not None and eval_mass_y is not None:
            try:
                return (int(float(eval_mass_x)), int(float(eval_mass_y)))
            except (TypeError, ValueError):
                raise ValueError("eval_mass_x and eval_mass_y must be numeric")
        if cond_masses:
            return cond_masses[0]
        raise RuntimeError("No eval mass specified and no conditioning masses available")

    def _background_mass_value(self, cond_masses: tuple[tuple[int, int], ...]) -> tuple[int, int]:
        configured = self.parameters.get("background_mass_value")
        if configured is not None:
            pair = self._normalize_mass_pair(configured)
            if pair is None:
                raise ValueError("background_mass_value must contain two numeric entries (m_x, m_y)")
            return pair
        return self._resolve_eval_mass(cond_masses)

    def _append_mass_features(self, base_features: np.ndarray, mass_pair: tuple[int, int]) -> np.ndarray:
        mass_x, mass_y = mass_pair
        mass_features = np.column_stack([
            np.full(len(base_features), mass_x, dtype=np.float32),
            np.full(len(base_features), mass_y, dtype=np.float32),
        ])
        return np.concatenate([base_features, mass_features], axis=1)

    def _append_mass_features_per_event(
        self,
        base_features: np.ndarray,
        mass_x: np.ndarray,
        mass_y: np.ndarray,
    ) -> np.ndarray:
        mass_features = np.column_stack([
            np.asarray(mass_x, dtype=np.float32),
            np.asarray(mass_y, dtype=np.float32),
        ])
        return np.concatenate([base_features, mass_features], axis=1)

    def _background_process_names(self) -> Iterable[str]:
        return self.background_processes

    def datasets(self, config_inst: od.Config) -> set[od.Dataset]:
        cond_masses = self._conditioning_masses()
        cond_set = set(cond_masses)

        dataset_insts: set[od.Dataset] = set()
        found_masses: set[tuple[int, int]] = set()

        for process_name in XYH_SIGNAL_PROCESSES:
            pair = self._normalize_mass_pair(process_name)
            if pair is None or pair not in cond_set:
                continue
            ds_for_proc = self._datasets_for_process(config_inst, process_name)
            if ds_for_proc:
                found_masses.add(pair)
            dataset_insts.update(ds_for_proc)

        missing = cond_set - found_masses
        if missing:
            missing_str = ", ".join(f"x{mx}_y{my}" for mx, my in sorted(missing))
            if self.parameters.get("skip_missing_masses", True):
                logger.warning("Skipping missing signal masses in training: %s", missing_str)
            else:
                raise RuntimeError(f"No training datasets found for signal masses: {missing_str}")

        for process_name in self._background_process_names():
            dataset_insts.update(self._datasets_for_process(config_inst, process_name))

        return dataset_insts

    def uses(self, config_inst: od.Config) -> set[Route | str]:
        routes = set(map(Route, self.feature_routes))
        routes.add(Route("category_ids"))
        return routes

    def produces(self, config_inst: od.Config) -> set[Route | str]:
        return {
            Route(f"{self.cls_name}.score"),
            Route(f"{self.cls_name}.score_signal"),
            Route(f"{self.cls_name}.score_rest"),
            Route("category_ids"),
        }

    def _eval_mass_affects_training(self) -> bool:
        mode = str(self.parameters.get("background_mass_mode", "random")).strip().lower()
        return mode == "fixed" and self.parameters.get("background_mass_value") is None

    def _training_parameter_pairs(self) -> list[tuple[str, Any]]:
        pairs = sorted(self.parameters.items())
        if self._eval_mass_affects_training():
            return pairs

        eval_only = {"eval_mass_x", "eval_mass_y"}
        return [(name, value) for name, value in pairs if name not in eval_only]

    def _training_model_repr(self) -> str:
        pairs = self._training_parameter_pairs()
        if not pairs:
            return self.cls_name

        joined = "__".join(
            f"{name}_{self._format_value(value)}"
            for name, value in pairs
        )
        return f"{self.cls_name}__{law.util.create_hash(joined)}"

    def output(self, task: law.Task) -> law.FileSystemDirectoryTarget:
        training_model_repr = self._training_model_repr()

        def training_store_parts_modifier(_task, parts):
            if "ml_model" in parts:
                parts["ml_model"] = f"ml__{training_model_repr}"
            return parts

        return task.target(
            f"mlmodel_f{task.branch}of{self.folds}",
            dir=True,
            store_parts_modifier=training_store_parts_modifier,
        )

    def open_model(self, target: law.FileSystemDirectoryTarget) -> "tf.keras.models.Model":
        return target.load(formatter="tf_keras_model")

    @staticmethod
    def _stack_events(parquet_targets: list[dict[str, law.FileSystemFileTarget]]) -> ak.Array:
        arrays = []
        for target in parquet_targets:
            file_target = target["mlevents"]
            if not file_target.exists():
                continue
            arrays.append(ak.from_parquet(file_target.abspath))
        if not arrays:
            return ak.Array([])
        return ak.concatenate(arrays, axis=0)

    @staticmethod
    def _scalar_feature(events: ak.Array, name: str, default: float = 0.0) -> np.ndarray:
        if name in events.fields:
            return np.asarray(ak.to_numpy(getattr(events, name)), dtype=np.float32)
        return np.full(len(events), default, dtype=np.float32)

    def _select_feature_names(self, available_names: tuple[str, ...]) -> tuple[str, ...]:
        feature_set_param = self.parameters.get("feature_set", "legacy")

        if isinstance(feature_set_param, (list, tuple)):
            selected_names = tuple(feature_set_param)
        else:
            key = str(feature_set_param).strip().lower()
            if not key or key == "all":
                return tuple(available_names)
            try:
                selected_names = self.feature_sets[key]
            except KeyError as exc:
                available_sets = ", ".join(sorted(self.feature_sets))
                raise ValueError(
                    f"Unknown feature set '{feature_set_param}'. Available: {available_sets}"
                ) from exc

        available = set(available_names)
        missing = [name for name in selected_names if name not in available]
        if missing:
            raise ValueError(
                f"Requested feature(s) {', '.join(missing)} are not available. "
                f"Known features: {', '.join(available_names)}"
            )
        return selected_names

    def _build_feature_matrix(self, events: ak.Array) -> tuple[np.ndarray, np.ndarray]:
        if len(events) == 0:
            return np.empty((0, 0), dtype=np.float32), np.empty((0,), dtype=np.float32)

        jets = events.Jet

        max_jets = 6
        padded_jet_pt = ak.fill_none(ak.pad_none(jets.pt, max_jets, axis=1, clip=True), 0.0)
        padded_jet_eta = ak.fill_none(ak.pad_none(jets.eta, max_jets, axis=1, clip=True), 0.0)
        padded_jet_btag = ak.fill_none(ak.pad_none(jets.btagDeepFlavB, max_jets, axis=1, clip=True), 0.0)

        jet_pts = [ak.to_numpy(padded_jet_pt[:, i]) for i in range(max_jets)]
        jet_etas = [ak.to_numpy(padded_jet_eta[:, i]) for i in range(max_jets)]
        jet_btags = [ak.to_numpy(padded_jet_btag[:, i]) for i in range(max_jets)]

        leading_jet_pt = jet_pts[0]
        subleading_jet_pt = jet_pts[1]
        jet3_pt = jet_pts[2]
        jet4_pt = jet_pts[3]
        jet5_pt = jet_pts[4]
        jet6_pt = jet_pts[5]

        leading_jet_eta = jet_etas[0]
        subleading_jet_eta = jet_etas[1]
        jet3_eta = jet_etas[2]
        jet4_eta = jet_etas[3]
        jet5_eta = jet_etas[4]
        jet6_eta = jet_etas[5]

        leading_jet_btag = jet_btags[0]
        subleading_jet_btag = jet_btags[1]
        jet3_btag = jet_btags[2]
        jet4_btag = jet_btags[3]
        jet5_btag = jet_btags[4]
        jet6_btag = jet_btags[5]

        ht = ak.to_numpy(ak.sum(jets.pt, axis=1, mask_identity=False))
        n_jets = ak.to_numpy(ak.num(jets.pt, axis=1))
        total_btag = ak.to_numpy(ak.sum(jets.btagDeepFlavB, axis=1, mask_identity=False))
        ht_additional = np.maximum(ht - leading_jet_pt - subleading_jet_pt, 0.0)
        n_additional_jets = np.maximum(n_jets - 2, 0).astype(np.float32, copy=False)

        if "Muon" in events.fields:
            mu_pt_raw = events.Muon.pt
            mu_eta_raw = events.Muon.eta
        else:
            mu_pt_raw = ak.Array([[]] * len(events))
            mu_eta_raw = ak.Array([[]] * len(events))

        if "Electron" in events.fields:
            el_pt_raw = events.Electron.pt
            el_eta_raw = events.Electron.eta
        else:
            el_pt_raw = ak.Array([[]] * len(events))
            el_eta_raw = ak.Array([[]] * len(events))

        mu_pt = ak.fill_none(ak.pad_none(mu_pt_raw, 1, axis=1, clip=True), 0.0)
        el_pt = ak.fill_none(ak.pad_none(el_pt_raw, 1, axis=1, clip=True), 0.0)
        mu_eta = ak.fill_none(ak.pad_none(mu_eta_raw, 1, axis=1, clip=True), 0.0)
        el_eta = ak.fill_none(ak.pad_none(el_eta_raw, 1, axis=1, clip=True), 0.0)

        muon_pt = ak.to_numpy(mu_pt[:, 0])
        electron_pt = ak.to_numpy(el_pt[:, 0])
        muon_eta = ak.to_numpy(mu_eta[:, 0])
        electron_eta = ak.to_numpy(el_eta[:, 0])

        n_muons = ak.to_numpy(ak.num(mu_pt_raw, axis=1))
        n_electrons = ak.to_numpy(ak.num(el_pt_raw, axis=1))

        if "Bjet" in events.fields:
            bjets = events.Bjet
        else:
            bjets = ak.Array([[]] * len(events))

        padded_bjet_pt = ak.fill_none(ak.pad_none(bjets.pt, 3, axis=1, clip=True), 0.0)
        padded_bjet_eta = ak.fill_none(ak.pad_none(bjets.eta, 3, axis=1, clip=True), 0.0)
        padded_bjet_btag = ak.fill_none(ak.pad_none(bjets.btagDeepFlavB, 3, axis=1, clip=True), 0.0)

        bjet_pts = [ak.to_numpy(padded_bjet_pt[:, i]) for i in range(3)]
        bjet_etas = [ak.to_numpy(padded_bjet_eta[:, i]) for i in range(3)]
        bjet_btags = [ak.to_numpy(padded_bjet_btag[:, i]) for i in range(3)]

        ht_bjets = ak.to_numpy(ak.sum(bjets.pt, axis=1, mask_identity=False))
        n_bjets = ak.to_numpy(ak.num(bjets.pt, axis=1)).astype(np.float32, copy=False)
        bjet_sum_btag = ak.to_numpy(ak.sum(bjets.btagDeepFlavB, axis=1, mask_identity=False))

        met_pt = self._scalar_feature(events, "MET.pt")
        met_phi = self._scalar_feature(events, "MET.phi")
        m_x = self._scalar_feature(events, "m_X")
        m_tt = self._scalar_feature(events, "m_tt")
        tt_pt = self._scalar_feature(events, "tt_pt")
        top_pt_feature = self._scalar_feature(events, "top_pt")
        top_mass_feature = self._scalar_feature(events, "top_mass")
        m_lead_b_feature = self._scalar_feature(events, "m_lead_b")
        lead_b_pt_feature = self._scalar_feature(events, "lead_b_pt")
        delta_r_jj = self._scalar_feature(events, "deltaR_jj")
        delta_r_qq = self._scalar_feature(events, "deltaR_qq")
        delta_r_bb = self._scalar_feature(events, "deltaR_bb")
        whad_mass = self._scalar_feature(events, "whad_mass")
        mlnu = self._scalar_feature(events, "mlnu")
        mtlnu = self._scalar_feature(events, "mtlnu")
        wboson_pt = self._scalar_feature(events, "wboson.pt")
        tt_bar_mass = self._scalar_feature(events, "tt_bar_mass")
        tt_bar_pt = self._scalar_feature(events, "tt_bar_pt")
        whad_pt = self._scalar_feature(events, "whad_pt")
        whad_eta = self._scalar_feature(events, "whad_eta")
        whad_phi = self._scalar_feature(events, "whad_phi")

        feature_values = OrderedDict([
            ("ht", ht),
            ("n_jets", n_jets),
            ("jet1_pt", leading_jet_pt),
            ("jet2_pt", subleading_jet_pt),
            ("jet1_eta", leading_jet_eta),
            ("jet2_eta", subleading_jet_eta),
            ("jet1_btag", leading_jet_btag),
            ("jet2_btag", subleading_jet_btag),
            ("muon1_pt", muon_pt),
            ("electron1_pt", electron_pt),
            ("muon1_eta", muon_eta),
            ("electron1_eta", electron_eta),
            ("n_muons", n_muons),
            ("n_electrons", n_electrons),
            ("met_pt", met_pt),
            ("met_phi", met_phi),
            ("m_x", m_x),
            ("m_tt", m_tt),
            ("tt_pt", tt_pt),
            ("top_pt", top_pt_feature),
            ("top_mass", top_mass_feature),
            ("m_lead_b", m_lead_b_feature),
            ("lead_b_pt", lead_b_pt_feature),
            ("delta_r_jj", delta_r_jj),
            ("delta_r_qq", delta_r_qq),
            ("delta_r_bb", delta_r_bb),
            ("whad_mass", whad_mass),
            ("mlnu", mlnu),
            ("mtlnu", mtlnu),
            ("wboson.pt", wboson_pt),
            ("tt_bar_mass", tt_bar_mass),
            ("tt_bar_pt", tt_bar_pt),
            ("whad_pt", whad_pt),
            ("whad_eta", whad_eta),
            ("whad_phi", whad_phi),
            ("ht_additional", ht_additional),
            ("total_btag", total_btag),
            ("n_additional_jets", n_additional_jets),
            ("jet3_pt", jet3_pt),
            ("jet4_pt", jet4_pt),
            ("jet5_pt", jet5_pt),
            ("jet6_pt", jet6_pt),
            ("jet3_eta", jet3_eta),
            ("jet4_eta", jet4_eta),
            ("jet5_eta", jet5_eta),
            ("jet6_eta", jet6_eta),
            ("jet3_btag", jet3_btag),
            ("jet4_btag", jet4_btag),
            ("jet5_btag", jet5_btag),
            ("jet6_btag", jet6_btag),
            ("ht_bjets", ht_bjets),
            ("n_bjets", n_bjets),
            ("bjet_sum_btag", bjet_sum_btag),
            ("bjet1_pt", bjet_pts[0]),
            ("bjet2_pt", bjet_pts[1]),
            ("bjet3_pt", bjet_pts[2]),
            ("bjet1_eta", bjet_etas[0]),
            ("bjet2_eta", bjet_etas[1]),
            ("bjet3_eta", bjet_etas[2]),
            ("bjet1_btag", bjet_btags[0]),
            ("bjet2_btag", bjet_btags[1]),
            ("bjet3_btag", bjet_btags[2]),
        ])

        selected_feature_names = self._select_feature_names(tuple(feature_values.keys()))
        features = np.column_stack([feature_values[name] for name in selected_feature_names]).astype(
            np.float32,
            copy=False,
        )

        if "normalization_weight" in events.fields:
            weights = ak.to_numpy(events.normalization_weight)
        else:
            weights = np.ones(len(events), dtype=np.float32)
        weights = np.asarray(weights, dtype=np.float32).reshape(-1)

        return features, weights

    def _build_model(self, input_dim: int) -> "tf.keras.models.Model":
        hidden_units = self.parameters.get("hidden_units", (1024, 1024, 1024, 1024))
        dropout = float(self.parameters.get("dropout", 0.0))
        l2_reg = float(self.parameters.get("l2_reg", 0.0))
        kernel_regularizer = tf.keras.regularizers.l2(l2_reg) if l2_reg > 0 else None

        inputs = tf.keras.Input(shape=(input_dim,))
        x = tf.keras.layers.BatchNormalization()(inputs)
        for units in hidden_units:
            x = tf.keras.layers.Dense(
                units,
                activation="relu",
                kernel_regularizer=kernel_regularizer,
            )(x)
            if dropout > 0:
                x = tf.keras.layers.Dropout(dropout)(x)
        outputs = tf.keras.layers.Dense(
            1,
            activation="sigmoid",
            kernel_regularizer=kernel_regularizer,
        )(x)
        model = tf.keras.Model(inputs=inputs, outputs=outputs)

        optimizer = tf.keras.optimizers.Adam(
            learning_rate=self.parameters.get("learning_rate", 5.0e-4),
            beta_1=0.9,
            beta_2=0.999,
            epsilon=1.0e-6,
            amsgrad=False,
        )

        model.compile(
            loss="binary_crossentropy",
            optimizer=optimizer,
            weighted_metrics=["binary_accuracy", "accuracy"],
        )

        return model

    def prepare_inputs(
        self,
        task: law.Task,
        input: dict[str, dict[str, list[dict[str, law.FileSystemFileTarget]]]],
    ) -> tuple[dict[str, np.ndarray], dict[str, np.ndarray]]:
        config_name = self.config_inst.name
        dataset_inputs = input["events"][config_name]
        cond_masses = self._conditioning_masses()
        signal_mass_map = self._signal_mass_map(cond_masses)
        signal_names = set(signal_mass_map)
        background_mode = self._background_mass_mode()
        background_mass_pair = self._background_mass_value(cond_masses) if background_mode == "fixed" else None
        seed = self.parameters.get("background_mass_seed")
        if seed is not None:
            if isinstance(seed, (list, tuple, np.ndarray)):
                seed = tuple(maybe_int(value) for value in seed)
                if not all(isinstance(value, (int, np.integer)) for value in seed):
                    raise ValueError("background_mass_seed must be an int or sequence of ints.")
            else:
                seed = maybe_int(seed)
                if not isinstance(seed, (int, np.integer)):
                    raise ValueError("background_mass_seed must be an int or sequence of ints.")
        rng = np.random.default_rng(seed)

        if self.parameters.get("skip_missing_masses", True):
            available_mass_set = set(signal_mass_map.values())
            cond_masses = tuple(pair for pair in cond_masses if pair in available_mass_set)
            if not cond_masses:
                raise RuntimeError("No signal masses with available datasets remain after skipping missing masses.")

        training_categories = self.parameters.get("training_categories")
        training_category_ids = None
        if training_categories:
            categories = law.util.make_list(training_categories)
            leaf_ids: list[int] = []
            for cat_name in categories:
                cat_inst = self.config_inst.get_category(cat_name)
                leaf_insts = cat_inst.get_leaf_categories() or [cat_inst]
                leaf_ids.extend(leaf.id for leaf in leaf_insts)
            training_category_ids = sorted(set(leaf_ids))

        features_list: list[np.ndarray] = []
        weights_list: list[np.ndarray] = []
        labels_list: list[np.ndarray] = []

        for dataset_name, targets in dataset_inputs.items():
            events = self._stack_events(targets)
            if len(events) == 0:
                continue
            if training_category_ids:
                if "category_ids" not in events.fields:
                    raise RuntimeError(
                        "training_categories requires 'category_ids' to be present in mlevents",
                    )
                if hasattr(ak, "isin"):
                    mask = ak.any(ak.isin(events.category_ids, training_category_ids), axis=1)
                else:
                    training_set = set(training_category_ids)
                    mask = ak.Array([
                        any(cat_id in training_set for cat_id in row)
                        for row in ak.to_list(events.category_ids)
                    ])
                events = events[mask]
                if len(events) == 0:
                    continue

            base_features, weights = self._build_feature_matrix(events)
            if base_features.size == 0:
                continue

            if dataset_name in signal_names:
                mass_pair = signal_mass_map[dataset_name]
                label_value = 1.0
                features = self._append_mass_features(base_features, mass_pair)
                features_list.append(features)
                weights_list.append(weights)
                labels_list.append(np.full(len(features), label_value, dtype=np.float32))
            else:
                label_value = 0.0
                if background_mode == "duplicate":
                    mass_pairs = cond_masses
                    for mass_pair in mass_pairs:
                        features = self._append_mass_features(base_features, mass_pair)
                        features_list.append(features)
                        weights_list.append(weights)
                        labels_list.append(np.full(len(features), label_value, dtype=np.float32))
                elif background_mode == "random":
                    if not cond_masses:
                        raise RuntimeError("conditioning_masses is required for random background mass mode.")
                    mass_x_choices = np.array([pair[0] for pair in cond_masses], dtype=np.int64)
                    mass_y_choices = np.array([pair[1] for pair in cond_masses], dtype=np.int64)
                    rand_idx = rng.integers(0, len(cond_masses), size=len(base_features))
                    features = self._append_mass_features_per_event(
                        base_features,
                        mass_x_choices[rand_idx],
                        mass_y_choices[rand_idx],
                    )
                    features_list.append(features)
                    weights_list.append(weights)
                    labels_list.append(np.full(len(features), label_value, dtype=np.float32))
                else:
                    mass_pairs = (background_mass_pair,)
                    for mass_pair in mass_pairs:
                        features = self._append_mass_features(base_features, mass_pair)
                        features_list.append(features)
                        weights_list.append(weights)
                        labels_list.append(np.full(len(features), label_value, dtype=np.float32))

        if not features_list:
            raise RuntimeError("No events available for training. Did PrepareMLEvents run?")

        x_all = np.concatenate(features_list, axis=0)
        w_all = np.concatenate(weights_list, axis=0)
        y_all = np.concatenate(labels_list, axis=0)

        if self.parameters.get("eqweight", True):
            signal_mask = y_all == 1.0
            bkg_mask = ~signal_mask
            sum_sig = np.sum(w_all[signal_mask])
            sum_bkg = np.sum(w_all[bkg_mask])
            weights_scaler = min(sum_sig, sum_bkg)
            w_all[signal_mask] *= weights_scaler / sum_sig
            w_all[bkg_mask] *= weights_scaler / sum_bkg

        shuffle_indices = np.arange(len(w_all))
        rng = np.random.default_rng()
        rng.shuffle(shuffle_indices)

        validation_fraction = float(self.parameters.get("validation_split", 0.0))
        n_validation_events = int(validation_fraction * len(w_all))
        n_validation_events = max(0, min(n_validation_events, len(w_all) - 1))

        x_all = x_all[shuffle_indices]
        w_all = w_all[shuffle_indices]
        y_all = y_all[shuffle_indices]

        train = {
            "inputs": x_all[n_validation_events:],
            "weights": w_all[n_validation_events:],
            "target": y_all[n_validation_events:],
        }
        validation = {
            "inputs": x_all[:n_validation_events],
            "weights": w_all[:n_validation_events],
            "target": y_all[:n_validation_events],
        }

        return train, validation

    def train(
        self,
        task: law.Task,
        input: dict[str, dict[str, list[dict[str, law.FileSystemFileTarget]]]],
        output: law.FileSystemDirectoryTarget,
    ) -> None:
        # Avoid GPU probing; run CPU-only in all environments.

        train, validation = self.prepare_inputs(task, input)

        for key in train.keys():
            if np.any(~np.isfinite(train[key])):
                raise Exception(f"Non-finite values found in training {key}")
            if np.any(~np.isfinite(validation[key])):
                raise Exception(f"Non-finite values found in validation {key}")

        n_inputs = train["inputs"].shape[1]
        model = self._build_model(n_inputs)

        has_validation = len(validation["inputs"]) > 0
        monitor = "val_loss" if has_validation else "loss"

        patience = int(self.parameters.get("patience", 0))
        callbacks = []
        if patience > 0:
            callbacks.append(tf.keras.callbacks.EarlyStopping(
                monitor=monitor,
                mode="min",
                min_delta=float(self.parameters.get("early_stopping_min_delta", 1.0e-4)),
                patience=patience,
                start_from_epoch=0,
                restore_best_weights=True,
            ))

        reduce_lr_factor = float(self.parameters.get("reduce_lr_factor", 0.0))
        reduce_lr_patience = int(self.parameters.get("reduce_lr_patience", 0))
        if reduce_lr_factor > 0 and reduce_lr_patience > 0:
            callbacks.append(tf.keras.callbacks.ReduceLROnPlateau(
                monitor=monitor,
                mode="min",
                min_delta=float(self.parameters.get("reduce_lr_min_delta", 1.0e-3)),
                factor=reduce_lr_factor,
                patience=reduce_lr_patience,
            ))

        batch_size = int(self.parameters.get("batch_size", 500))
        epochs = int(self.parameters.get("epochs", 500))

        with tf.device("CPU"):
            tf_train = tf.data.Dataset.from_tensor_slices(
                (train["inputs"], train["target"], train["weights"]),
            ).batch(batch_size)

            validation_data = None
            if has_validation:
                tf_validate = tf.data.Dataset.from_tensor_slices(
                    (validation["inputs"], validation["target"], validation["weights"]),
                ).batch(batch_size)
                validation_data = tf_validate

        model.fit(
            tf_train,
            validation_data=validation_data,
            epochs=epochs,
            callbacks=callbacks,
            verbose=2,
        )

        output.parent.touch()
        model.save(output.path)
        with open(f"{output.path}/model_history.pkl", "wb") as f:
            pickle.dump(model.history.history, f)

    def _store_scores(self, events: ak.Array, scores: np.ndarray) -> ak.Array:
        scores = np.asarray(scores, dtype=np.float32).reshape(-1)
        score_signal = scores
        score_rest = np.clip(1.0 - scores, 0.0, 1.0).astype(np.float32)

        events = set_ak_column(events, f"{self.cls_name}.score", score_signal)
        events = set_ak_column(events, f"{self.cls_name}.score_signal", score_signal)
        events = set_ak_column(events, f"{self.cls_name}.score_rest", score_rest)
        return events

    def evaluate(
        self,
        task: law.Task,
        events: ak.Array,
        models: list[Any],
        fold_indices: ak.Array,
        events_used_in_training: bool = False,
    ) -> ak.Array:
        if len(events) == 0:
            return events

        base_features, _ = self._build_feature_matrix(events)
        if base_features.size == 0:
            return self._store_scores(events, np.zeros(len(events), dtype=np.float32))

        mass_pair = self._resolve_eval_mass(self._conditioning_masses())
        features = self._append_mass_features(base_features, mass_pair)

        predictions = []
        for model in models:
            prediction = model.predict_on_batch(features)
            prediction = np.squeeze(prediction, axis=-1)
            predictions.append(ak.from_numpy(prediction))

        outputs = ak.ones_like(predictions[0]) * -1
        for i in range(self.folds):
            idx = fold_indices == i
            outputs = ak.where(idx, predictions[i], outputs)

        return self._store_scores(events, ak.to_numpy(outputs))

    def training_configs(self, requested_configs):
        return requested_configs


XYH_PNN = XYHPNNModel.derive("xyh_pnn_parameterized")
