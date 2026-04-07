# coding: utf-8

"""
XYH signal-versus-background binary classifiers.
"""

from __future__ import annotations

import json
import os
import shutil
from collections import OrderedDict
from typing import Any, Iterable

import law
import numpy as np
import order as od
import re

from columnflow.ml import MLModel
from columnflow.tasks.framework.parameters import SettingsParameter
from columnflow.util import maybe_import, dev_sandbox
from columnflow.columnar_util import Route, set_ak_column

ak = maybe_import("awkward")
tf = maybe_import("tensorflow")

law.contrib.load("tensorflow")

from xyh.inference.signals import XYH_SIGNAL_PROCESSES

logger = law.logger.get_logger(__name__)


class XYHBinaryModel(MLModel):
    """Binary NN classifier to separate XYH signal from dominant backgrounds."""

    require_mass_hypothesis: bool = True
    mass_x: int | None = None
    mass_y: int | None = None

    # allow passing the mass hypothesis via derive/constructor kwargs
    init_attributes = MLModel.init_attributes + ["mass_x", "mass_y"]

    # configurable defaults exposed via ``self.parameters``
    default_parameters = OrderedDict([
        ("hidden_units", (512, 512, 256, 128)),
        ("dropout", 0.2),
        ("learning_rate", 5.0e-4),
        ("batch_size", 2048),
        ("epochs", 30),
        ("patience", 8),
        ("validation_split", 0.2),
        ("feature_set", "legacy"),
        ("reduce_lr_factor", 0.5),
        ("reduce_lr_patience", 3),
        ("reduce_lr_min_delta", 1.0e-4),
        ("training_categories", None),
    ])

    # process names registered in the Run-3 configuration that we use as backgrounds
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

    # column routes needed during training/evaluation
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
        #"m_bb",
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
        #"m_bb",
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
        #"m_bb",
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
        super().__init__(*args, **kwargs)

        if ak is None or tf is None:
            raise RuntimeError("XYHBinaryModel requires awkward and tensorflow to be available in the sandbox")

        env_settings = os.environ.get("ML_SETTINGS")
        if env_settings:
            try:
                parsed = SettingsParameter().parse(env_settings)
            except Exception as exc:
                logger.warning("Failed to parse ML_SETTINGS for XYHBinaryModel: %s", exc)
                parsed = {}
            for key, value in dict(parsed).items():
                if key not in self.parameters:
                    self.parameters[key] = value

        if self.require_mass_hypothesis and (
            getattr(self, "mass_x", None) is None or getattr(self, "mass_y", None) is None
        ):
            raise ValueError("XYHBinaryModel requires 'mass_x' and 'mass_y' parameters")

        for name, value in self.default_parameters.items():
            self.parameters.setdefault(name, value)

    # ------------------------------------------------------------------
    # configuration hooks
    # ------------------------------------------------------------------

    def setup(self) -> None:
        # add the score column if it is not present yet
        score_name = f"{self.cls_name}.score"
        if score_name not in self.config_inst.variables:
            self.config_inst.add_variable(
                name=score_name,
                null_value=-1.0,
                binning=(20, 0.0, 1.0),
                x_title=f"{self.cls_name} score",
            )

    def sandbox(self, task: law.Task) -> str:
        # re-use the development sandbox; adjust if a dedicated ML sandbox exists
        return dev_sandbox("bash::$XYH_BASE/sandboxes/example.sh")

    # ------------------------------------------------------------------
    # dataset and column bookkeeping
    # ------------------------------------------------------------------

    def _signal_process_name(self) -> str:
        return f"xyh_sl_x{self.mass_x}_y{self.mass_y}"

    def _background_process_names(self) -> Iterable[str]:
        # keep ordering stable for deterministic behaviour
        return self.background_processes

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

    def datasets(self, config_inst: od.Config) -> set[od.Dataset]:
        dataset_insts: set[od.Dataset] = set()

        # required signal sample
        signal_datasets = self._datasets_for_process(self.config_inst, self._signal_process_name())
        if not signal_datasets:
            raise RuntimeError(
                f"no training datasets found for signal process '{self._signal_process_name()}'"
            )
        dataset_insts.update(signal_datasets)

        # add backgrounds if they exist in the config
        for process_name in self._background_process_names():
            dataset_insts.update(self._datasets_for_process(self.config_inst, process_name))

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

    # ------------------------------------------------------------------
    # I/O helpers
    # ------------------------------------------------------------------

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

        # handle optional lepton collections
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
            bjet_collection = events.Bjet
            bjet_pt_raw = bjet_collection.pt
            bjet_eta_raw = bjet_collection.eta
            bjet_fields = set(getattr(bjet_collection, "fields", []))
            if "btagDeepFlavB" in bjet_fields:
                bjet_btag_raw = bjet_collection.btagDeepFlavB
            else:
                bjet_btag_raw = ak.Array([[]] * len(events))
        else:
            bjet_pt_raw = ak.Array([[]] * len(events))
            bjet_eta_raw = ak.Array([[]] * len(events))
            bjet_btag_raw = ak.Array([[]] * len(events))

        max_bjets = 3
        padded_bjet_pt = ak.fill_none(ak.pad_none(bjet_pt_raw, max_bjets, axis=1, clip=True), 0.0)
        padded_bjet_eta = ak.fill_none(ak.pad_none(bjet_eta_raw, max_bjets, axis=1, clip=True), 0.0)
        padded_bjet_btag = ak.fill_none(ak.pad_none(bjet_btag_raw, max_bjets, axis=1, clip=True), 0.0)

        bjet_pts = [ak.to_numpy(padded_bjet_pt[:, i]) for i in range(max_bjets)]
        bjet_etas = [ak.to_numpy(padded_bjet_eta[:, i]) for i in range(max_bjets)]
        bjet_btags = [ak.to_numpy(padded_bjet_btag[:, i]) for i in range(max_bjets)]

        ht_bjets = ak.to_numpy(ak.sum(bjet_pt_raw, axis=1, mask_identity=False))
        n_bjets = np.asarray(ak.to_numpy(ak.num(bjet_pt_raw, axis=1)), dtype=np.float32)
        bjet_sum_btag = ak.to_numpy(ak.sum(bjet_btag_raw, axis=1, mask_identity=False))

        met_pt = ak.to_numpy(events.MET.pt)
        met_phi = ak.to_numpy(events.MET.phi)

        m_x = self._scalar_feature(events, "m_X")
        m_tt = self._scalar_feature(events, "m_tt")
        tt_pt = self._scalar_feature(events, "tt_pt")
        top_pt_feature = self._scalar_feature(events, "top_pt")
        top_mass_feature = self._scalar_feature(events, "top_mass")
        #m_bb = self._scalar_feature(events, "m_bb")
        m_lead_b_feature = self._scalar_feature(events, "m_lead_b")
        lead_b_pt_feature = self._scalar_feature(events, "lead_b_pt")
        delta_r_jj = self._scalar_feature(events, "deltaR_jj")
        delta_r_qq = self._scalar_feature(events, "deltaR_qq")
        delta_r_bb = self._scalar_feature(events, "deltaR_bb")

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
            #("m_bb", m_bb),
            ("m_lead_b", m_lead_b_feature),
            ("lead_b_pt", lead_b_pt_feature),
            ("delta_r_jj", delta_r_jj),
            ("delta_r_qq", delta_r_qq),
            ("delta_r_bb", delta_r_bb),
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
        features = np.column_stack([feature_values[name] for name in selected_feature_names]).astype(np.float32, copy=False)

        if "normalization_weight" in events.fields:
            weights = ak.to_numpy(events.normalization_weight)
        else:
            weights = np.ones(len(events), dtype=np.float32)
        weights = np.asarray(weights, dtype=np.float32).reshape(-1)

        return features, weights

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

    # ------------------------------------------------------------------
    # ML hooks
    # ------------------------------------------------------------------

    def output(self, task: law.Task) -> law.FileSystemFileTarget:
        return task.target(f"mlmodel_f{task.fold}of{self.folds}.keras")

    def open_model(self, target: law.FileSystemFileTarget) -> tf.keras.models.Model:
        return target.load(formatter="tf_keras_model")

    def _build_model(self, input_dim: int) -> tf.keras.models.Model:
        hidden_units = self.parameters.get("hidden_units", (512, 512, 256, 128))
        dropout = float(self.parameters.get("dropout", 0.0))
        inputs = tf.keras.Input(shape=(input_dim,))
        x = tf.keras.layers.BatchNormalization()(inputs)
        for units in hidden_units:
            x = tf.keras.layers.Dense(units, activation="relu")(x)
            if dropout > 0:
                x = tf.keras.layers.Dropout(dropout)(x)
        outputs = tf.keras.layers.Dense(1, activation="sigmoid")(x)
        model = tf.keras.Model(inputs=inputs, outputs=outputs)

        model.compile(
            optimizer=tf.keras.optimizers.Adam(learning_rate=self.parameters.get("learning_rate", 1.0e-3)),
            loss="binary_crossentropy",
            metrics=["accuracy"],
        )
        return model

    def train(
        self,
        task: law.Task,
        input: dict[str, dict[str, list[dict[str, law.FileSystemFileTarget]]]],
        output: law.FileSystemFileTarget,
    ) -> None:
        config_name = self.config_inst.name
        dataset_inputs = input["events"][config_name]

        features_list = []
        weights_list = []
        labels_list = []

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

        signal_datasets = self._datasets_for_process(self.config_inst, self._signal_process_name())
        if not signal_datasets:
            raise RuntimeError(
                f"no training datasets found for signal process '{self._signal_process_name()}'"
            )
        signal_names = {dataset.name for dataset in signal_datasets}

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
            features, weights = self._build_feature_matrix(events)
            if features.size == 0:
                continue
            label_value = 1.0 if dataset_name in signal_names else 0.0

            features_list.append(features)
            weights_list.append(weights)
            labels_list.append(np.full(len(features), label_value, dtype=np.float32))

        if not features_list:
            raise RuntimeError("No events available for training. Did PrepareMLEvents run?")

        x_all = np.concatenate(features_list, axis=0)
        w_all = np.concatenate(weights_list, axis=0)
        y_all = np.concatenate(labels_list, axis=0)

        # rescale sample weights so signal and background contribute equally to the loss
        signal_mask = y_all == 1.0
        bkg_mask = ~signal_mask
        sum_sig = np.sum(w_all[signal_mask])
        sum_bkg = np.sum(w_all[bkg_mask])
        if sum_sig <= 0 or sum_bkg <= 0:
            raise RuntimeError(
                "Training fold is missing signal or background events; cannot balance classes."
            )
        if sum_sig > 0 and sum_bkg > 0:
            target = 0.5  # arbitrary common target weight per class
            w_all[signal_mask] *= target / sum_sig
            w_all[bkg_mask] *= target / sum_bkg

        validation_data: tuple[np.ndarray, np.ndarray, np.ndarray] | None = None
        validation_split = float(self.parameters.get("validation_split", 0.0))
        if 0.0 < validation_split < 1.0 and len(x_all) >= 2:
            val_size = int(len(x_all) * validation_split)
            val_size = max(val_size, 1)
            if val_size >= len(x_all):
                val_size = len(x_all) - 1
            indices = np.arange(len(x_all))
            seed_param = self.parameters.get("validation_seed")
            if seed_param is None:
                fold_value = getattr(task, "fold", None)
                if fold_value is not None:
                    try:
                        seed_param = int(fold_value)
                    except (TypeError, ValueError):
                        seed_param = None
            rng = np.random.default_rng(seed_param)
            rng.shuffle(indices)
            val_indices = indices[:val_size]
            train_indices = indices[val_size:]
            x_train = x_all[train_indices]
            y_train = y_all[train_indices]
            w_train = w_all[train_indices]
            x_val = x_all[val_indices]
            y_val = y_all[val_indices]
            w_val = w_all[val_indices]
            validation_data = (x_val, y_val, w_val)
        else:
            x_train = x_all
            y_train = y_all
            w_train = w_all

        model = self._build_model(input_dim=x_train.shape[1])

        # derive inverse-frequency class weights to balance signal vs. background
        unique_classes = np.unique(y_train)
        class_weight = {}
        total_weight = np.sum(w_train)
        num_classes = len(unique_classes) if len(unique_classes) > 0 else 1
        for cls in unique_classes:
            cls_mask = (y_train == cls)
            cls_weight = np.sum(w_train[cls_mask])
            if cls_weight > 0:
                class_weight[int(cls)] = float(total_weight / (num_classes * cls_weight))

        callbacks = []
        patience = int(self.parameters.get("patience", 0))
        if patience > 0:
            callbacks.append(tf.keras.callbacks.EarlyStopping(
                monitor="val_loss" if validation_data is not None else "loss",
                patience=patience,
                restore_best_weights=True,
            ))

        reduce_lr_factor = float(self.parameters.get("reduce_lr_factor", 0.0))
        reduce_lr_patience = int(self.parameters.get("reduce_lr_patience", 0))
        if reduce_lr_factor > 0 and reduce_lr_patience > 0:
            callbacks.append(tf.keras.callbacks.ReduceLROnPlateau(
                monitor="val_loss" if validation_data is not None else "loss",
                factor=reduce_lr_factor,
                patience=reduce_lr_patience,
                min_delta=float(self.parameters.get("reduce_lr_min_delta", 1.0e-4)),
                verbose=1,
            ))

        history = model.fit(
            x_train,
            y_train,
            sample_weight=w_train,
            epochs=int(self.parameters.get("epochs", 20)),
            batch_size=int(self.parameters.get("batch_size", 1024)),
            shuffle=True,
            verbose=2,
            callbacks=callbacks,
            validation_data=validation_data,
            class_weight=class_weight if class_weight else None,
        )

        history_target = task.target(f"mlmodel_f{task.fold}of{self.folds}_training_history.json")
        history_target.parent.touch()
        history_data = {name: [float(v) for v in values] for name, values in history.history.items()}
        with history_target.open("w") as history_file:
            json.dump({
                "metrics": history_data,
                "params": history.params,
                "epoch": history.epoch,
            }, history_file, indent=2, sort_keys=True)

        output.parent.touch()
        if os.path.isdir(output.abspath):
            shutil.rmtree(output.abspath)
        elif os.path.exists(output.abspath):
            os.remove(output.abspath)

        output.dump(model, formatter="tf_keras_model")

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

        features, _ = self._build_feature_matrix(events)
        if features.size == 0:
            return self._store_scores(events, np.zeros(len(events), dtype=np.float32))

        # average over folds
        predictions = np.zeros(len(features), dtype=np.float32)
        for model in models:
            predictions += np.squeeze(model.predict(features, verbose=0), axis=-1)
        predictions /= max(len(models), 1)

        return self._store_scores(events, predictions)

    def _store_scores(self, events: ak.Array, scores: np.ndarray) -> ak.Array:
        """
        Helper that stores the raw score plus signal/rest split needed for CM plots.
        """
        scores = np.asarray(scores, dtype=np.float32)
        score_signal = scores
        score_rest = np.clip(1.0 - scores, 0.0, 1.0).astype(np.float32)

        events = set_ak_column(events, f"{self.cls_name}.score", score_signal)
        events = set_ak_column(events, f"{self.cls_name}.score_signal", score_signal)
        events = set_ak_column(events, f"{self.cls_name}.score_rest", score_rest)
        return events


class XYHParameterizedBinaryModel(XYHBinaryModel):
    """
    Mass-conditioned binary NN that trains on multiple signal points and appends (m_x, m_y) features.
    """

    require_mass_hypothesis = False
    _last_conditioning_masses: tuple[tuple[int, int], ...] | None = None

    default_parameters = OrderedDict(XYHBinaryModel.default_parameters)
    default_parameters.update([
        ("conditioning_masses", None),
        ("background_mass_mode", "duplicate"),
        ("background_mass_value", None),
        ("eval_mass_x", None),
        ("eval_mass_y", None),
        ("skip_missing_masses", True),
        ("require_conditioning_masses", True),
    ])

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        logger.info("XYHParameterizedBinaryModel init parameters=%s", dict(self.parameters))

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

    def _conditioning_masses(self) -> tuple[tuple[int, int], ...]:
        raw = self.parameters.get("conditioning_masses")
        logger.info("XYHParameterizedBinaryModel conditioning_masses raw=%r", raw)
        if raw is None:
            raise RuntimeError(
                "conditioning_masses not provided; set --ml-model-settings "
                "conditioning_masses=... to run the parameterized model."
            )
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
        result = tuple(ordered)
        self._last_conditioning_masses = result
        logger.info(
            "XYHParameterizedBinaryModel using conditioning_masses=%s",
            ", ".join(f"x{mx}_y{my}" for mx, my in result),
        )
        return result

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
        if mode not in {"duplicate", "fixed"}:
            raise ValueError("background_mass_mode must be 'duplicate' or 'fixed'")
        return mode

    def _background_mass_value(self, cond_masses: tuple[tuple[int, int], ...]) -> tuple[int, int]:
        configured = self.parameters.get("background_mass_value")
        if configured is not None:
            pair = self._normalize_mass_pair(configured)
            if pair is None:
                raise ValueError("background_mass_value must contain two numeric entries (m_x, m_y)")
            return pair
        return self._resolve_eval_mass(cond_masses)

    def _resolve_eval_mass(self, cond_masses: tuple[tuple[int, int], ...]) -> tuple[int, int]:
        eval_x = self.parameters.get("eval_mass_x")
        eval_y = self.parameters.get("eval_mass_y")
        pair = None
        if eval_x is not None and eval_y is not None:
            pair = self._normalize_mass_pair((eval_x, eval_y))
        if pair is None:
            attr_pair = self._normalize_mass_pair((getattr(self, "mass_x", None), getattr(self, "mass_y", None)))
            if attr_pair is not None:
                pair = attr_pair
        if pair is None and cond_masses:
            pair = cond_masses[0]
        if pair is None:
            raise RuntimeError(
                "No evaluation mass available; set mass_x/mass_y or eval_mass_x/eval_mass_y."
            )
        if cond_masses and pair not in set(cond_masses):
            raise ValueError(
                f"Requested evaluation mass x{pair[0]}_y{pair[1]} not present in conditioning_masses."
            )
        return pair

    def _append_mass_features(self, base_features: np.ndarray, mass_pair: tuple[int, int]) -> np.ndarray:
        mass_x, mass_y = mass_pair
        mass_features = np.column_stack([
            np.full(len(base_features), mass_x, dtype=np.float32),
            np.full(len(base_features), mass_y, dtype=np.float32),
        ])
        return np.concatenate([base_features, mass_features], axis=1)

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

    def train(
        self,
        task: law.Task,
        input: dict[str, dict[str, list[dict[str, law.FileSystemFileTarget]]]],
        output: law.FileSystemFileTarget,
    ) -> None:
        config_name = self.config_inst.name
        dataset_inputs = input["events"][config_name]

        cond_masses = self._conditioning_masses()
        signal_mass_map = self._signal_mass_map(cond_masses)
        signal_names = set(signal_mass_map)
        background_mode = self._background_mass_mode()
        background_mass_pair = self._background_mass_value(cond_masses) if background_mode == "fixed" else None

        if self.parameters.get("skip_missing_masses", True):
            available_mass_set = set(signal_mass_map.values())
            cond_masses = tuple(pair for pair in cond_masses if pair in available_mass_set)
            if not cond_masses:
                raise RuntimeError("No signal masses with available datasets remain after skipping missing masses.")

        features_list: list[np.ndarray] = []
        weights_list: list[np.ndarray] = []
        labels_list: list[np.ndarray] = []

        for dataset_name, targets in dataset_inputs.items():
            events = self._stack_events(targets)
            if len(events) == 0:
                continue

            base_features, weights = super()._build_feature_matrix(events)
            if base_features.size == 0:
                continue

            if dataset_name in signal_names:
                mass_pairs = (signal_mass_map[dataset_name],)
                label_value = 1.0
            else:
                label_value = 0.0
                if background_mode == "duplicate":
                    mass_pairs = cond_masses
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

        signal_mask = y_all == 1.0
        bkg_mask = ~signal_mask
        sum_sig = np.sum(w_all[signal_mask])
        sum_bkg = np.sum(w_all[bkg_mask])
        if sum_sig <= 0 or sum_bkg <= 0:
            raise RuntimeError(
                "Training fold is missing signal or background events; cannot balance classes."
            )
        if sum_sig > 0 and sum_bkg > 0:
            target = 0.5
            w_all[signal_mask] *= target / sum_sig
            w_all[bkg_mask] *= target / sum_bkg

        validation_data: tuple[np.ndarray, np.ndarray, np.ndarray] | None = None
        validation_split = float(self.parameters.get("validation_split", 0.0))
        if 0.0 < validation_split < 1.0 and len(x_all) >= 2:
            val_size = int(len(x_all) * validation_split)
            val_size = max(val_size, 1)
            if val_size >= len(x_all):
                val_size = len(x_all) - 1
            indices = np.arange(len(x_all))
            seed_param = self.parameters.get("validation_seed")
            if seed_param is None:
                fold_value = getattr(task, "fold", None)
                if fold_value is not None:
                    try:
                        seed_param = int(fold_value)
                    except (TypeError, ValueError):
                        seed_param = None
            rng = np.random.default_rng(seed_param)
            rng.shuffle(indices)
            val_indices = indices[:val_size]
            train_indices = indices[val_size:]
            x_train = x_all[train_indices]
            y_train = y_all[train_indices]
            w_train = w_all[train_indices]
            x_val = x_all[val_indices]
            y_val = y_all[val_indices]
            w_val = w_all[val_indices]
            validation_data = (x_val, y_val, w_val)
        else:
            x_train = x_all
            y_train = y_all
            w_train = w_all

        model = self._build_model(input_dim=x_train.shape[1])

        unique_classes = np.unique(y_train)
        class_weight = {}
        total_weight = np.sum(w_train)
        num_classes = len(unique_classes) if len(unique_classes) > 0 else 1
        for cls in unique_classes:
            cls_mask = (y_train == cls)
            cls_weight = np.sum(w_train[cls_mask])
            if cls_weight > 0:
                class_weight[int(cls)] = float(total_weight / (num_classes * cls_weight))

        callbacks = []
        patience = int(self.parameters.get("patience", 0))
        if patience > 0:
            callbacks.append(tf.keras.callbacks.EarlyStopping(
                monitor="val_loss" if validation_data is not None else "loss",
                patience=patience,
                restore_best_weights=True,
            ))

        reduce_lr_factor = float(self.parameters.get("reduce_lr_factor", 0.0))
        reduce_lr_patience = int(self.parameters.get("reduce_lr_patience", 0))
        if reduce_lr_factor > 0 and reduce_lr_patience > 0:
            callbacks.append(tf.keras.callbacks.ReduceLROnPlateau(
                monitor="val_loss" if validation_data is not None else "loss",
                factor=reduce_lr_factor,
                patience=reduce_lr_patience,
                min_delta=float(self.parameters.get("reduce_lr_min_delta", 1.0e-4)),
                verbose=1,
            ))

        history = model.fit(
            x_train,
            y_train,
            sample_weight=w_train,
            epochs=int(self.parameters.get("epochs", 20)),
            batch_size=int(self.parameters.get("batch_size", 1024)),
            shuffle=True,
            verbose=2,
            callbacks=callbacks,
            validation_data=validation_data,
            class_weight=class_weight if class_weight else None,
        )

        history_target = task.target(f"mlmodel_f{task.fold}of{self.folds}_training_history.json")
        history_target.parent.touch()
        history_data = {name: [float(v) for v in values] for name, values in history.history.items()}
        with history_target.open("w") as history_file:
            json.dump({
                "metrics": history_data,
                "params": history.params,
                "epoch": history.epoch,
            }, history_file, indent=2, sort_keys=True)

        output.parent.touch()
        if os.path.isdir(output.abspath):
            shutil.rmtree(output.abspath)
        elif os.path.exists(output.abspath):
            os.remove(output.abspath)

        output.dump(model, formatter="tf_keras_model")

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

        base_features, _ = super()._build_feature_matrix(events)
        if base_features.size == 0:
            return self._store_scores(events, np.zeros(len(events), dtype=np.float32))

        mass_pair = self._resolve_eval_mass(self._conditioning_masses())
        features = self._append_mass_features(base_features, mass_pair)

        predictions = np.zeros(len(features), dtype=np.float32)
        for model in models:
            predictions += np.squeeze(model.predict(features, verbose=0), axis=-1)
        predictions /= max(len(models), 1)

        return self._store_scores(events, predictions)


def _register_parameterized_model() -> None:
    globals()["xyh_binary_parameterized"] = XYHParameterizedBinaryModel.derive(
        "xyh_binary_parameterized",
    )


def _register_additional_models(max_mass_x: int = 2000) -> None:
    """Derive binary models for all XYH signal hypotheses up to *max_mass_x*."""
    pattern = re.compile(r"xyh_sl_x(?P<x>\d+)_y(?P<y>\d+)")

    for process_name in XYH_SIGNAL_PROCESSES:
        match = pattern.fullmatch(process_name)
        if not match:
            continue

        mass_x = int(match.group("x"))
        mass_y = int(match.group("y"))
        if mass_x > max_mass_x:
            continue

        cls_name = f"xyh_binary_x{mass_x}_y{mass_y}"

        globals()[cls_name] = XYHBinaryModel.derive(
            cls_name,
            cls_dict={"mass_x": mass_x, "mass_y": mass_y},
        )


_register_parameterized_model()
_register_additional_models()
