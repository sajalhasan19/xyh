"""
Helpers for post-training feature-impact studies on XYH ML models.
"""

from __future__ import annotations

from collections import OrderedDict
import importlib
import json
from pathlib import Path
from typing import Any, Sequence

import awkward as ak
import numpy as np


BINARY_LEGACY_FEATURE_NAMES: tuple[str, ...] = (
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
    "m_h",
    "m_x_minus_m_h",
    "m_x_minus_m_tt",
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

BINARY_CURATED_FEATURE_NAMES: tuple[str, ...] = (
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
    "m_h",
    "m_x_minus_m_h",
    "m_x_minus_m_tt",
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

BINARY_FEATURE_SETS: dict[str, tuple[str, ...]] = {
    "legacy": BINARY_LEGACY_FEATURE_NAMES,
    "all": BINARY_LEGACY_FEATURE_NAMES,
    "curated": BINARY_CURATED_FEATURE_NAMES,
}

PNN_FEATURE_SETS: dict[str, tuple[str, ...]] = dict(BINARY_FEATURE_SETS)


def load_parquet_inputs(paths: Sequence[str | Path]) -> ak.Array:
    arrays = []
    for raw_path in paths:
        path = Path(raw_path).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"Input file not found: {path}")
        arrays.append(ak.from_parquet(path))
    if not arrays:
        return ak.Array([])
    if len(arrays) == 1:
        return arrays[0]
    return ak.concatenate(arrays, axis=0)


def _empty_nested_array(n_events: int) -> ak.Array:
    return ak.Array([[]] * n_events)


def _resolve_field(events: ak.Array, route: str, default: float = 0.0) -> np.ndarray:
    current: Any = events
    for part in route.split("."):
        fields = set(getattr(current, "fields", []))
        if part not in fields:
            return np.full(len(events), default, dtype=np.float32)
        current = getattr(current, part)
    return np.asarray(ak.to_numpy(current), dtype=np.float32)


def _pad_first_axis(values: ak.Array, width: int) -> list[np.ndarray]:
    padded = ak.fill_none(ak.pad_none(values, width, axis=1, clip=True), 0.0)
    return [np.asarray(ak.to_numpy(padded[:, i]), dtype=np.float32) for i in range(width)]


def _select_feature_names(
    feature_set: str | Sequence[str],
    available_names: tuple[str, ...],
    feature_sets: dict[str, tuple[str, ...]],
) -> tuple[str, ...]:
    if isinstance(feature_set, (list, tuple)):
        selected_names = tuple(str(name) for name in feature_set)
    else:
        key = str(feature_set).strip().lower()
        if not key or key == "all":
            return tuple(available_names)
        try:
            selected_names = feature_sets[key]
        except KeyError as exc:
            raise ValueError(
                f"Unknown feature set '{feature_set}'. Available: {', '.join(sorted(feature_sets))}"
            ) from exc

    available = set(available_names)
    missing = [name for name in selected_names if name not in available]
    if missing:
        raise ValueError(
            f"Requested feature(s) {', '.join(missing)} are not available. "
            f"Known features: {', '.join(available_names)}"
        )
    return selected_names


def build_binary_feature_matrix(
    events: ak.Array,
    feature_set: str | Sequence[str] = "legacy",
) -> tuple[np.ndarray, np.ndarray, tuple[str, ...]]:
    if len(events) == 0:
        return np.empty((0, 0), dtype=np.float32), np.empty((0,), dtype=np.float32), tuple()

    jets = events.Jet
    jet_pts = _pad_first_axis(jets.pt, 6)
    jet_etas = _pad_first_axis(jets.eta, 6)
    jet_btags = _pad_first_axis(jets.btagDeepFlavB, 6)

    ht = np.asarray(ak.to_numpy(ak.sum(jets.pt, axis=1, mask_identity=False)), dtype=np.float32)
    n_jets = np.asarray(ak.to_numpy(ak.num(jets.pt, axis=1)), dtype=np.float32)
    total_btag = np.asarray(ak.to_numpy(ak.sum(jets.btagDeepFlavB, axis=1, mask_identity=False)), dtype=np.float32)
    ht_additional = np.maximum(ht - jet_pts[0] - jet_pts[1], 0.0).astype(np.float32, copy=False)
    n_additional_jets = np.maximum(n_jets - 2, 0).astype(np.float32, copy=False)

    if "Muon" in events.fields:
        mu_pt_raw = events.Muon.pt
        mu_eta_raw = events.Muon.eta
    else:
        mu_pt_raw = _empty_nested_array(len(events))
        mu_eta_raw = _empty_nested_array(len(events))

    if "Electron" in events.fields:
        el_pt_raw = events.Electron.pt
        el_eta_raw = events.Electron.eta
    else:
        el_pt_raw = _empty_nested_array(len(events))
        el_eta_raw = _empty_nested_array(len(events))

    muon_pt = _pad_first_axis(mu_pt_raw, 1)[0]
    muon_eta = _pad_first_axis(mu_eta_raw, 1)[0]
    electron_pt = _pad_first_axis(el_pt_raw, 1)[0]
    electron_eta = _pad_first_axis(el_eta_raw, 1)[0]
    n_muons = np.asarray(ak.to_numpy(ak.num(mu_pt_raw, axis=1)), dtype=np.float32)
    n_electrons = np.asarray(ak.to_numpy(ak.num(el_pt_raw, axis=1)), dtype=np.float32)

    if "Bjet" in events.fields:
        bjet_collection = events.Bjet
        bjet_fields = set(getattr(bjet_collection, "fields", []))
        bjet_pt_raw = bjet_collection.pt if "pt" in bjet_fields else _empty_nested_array(len(events))
        bjet_eta_raw = bjet_collection.eta if "eta" in bjet_fields else _empty_nested_array(len(events))
        if "btagDeepFlavB" in bjet_fields:
            bjet_btag_raw = bjet_collection.btagDeepFlavB
        else:
            bjet_btag_raw = _empty_nested_array(len(events))
    else:
        bjet_pt_raw = _empty_nested_array(len(events))
        bjet_eta_raw = _empty_nested_array(len(events))
        bjet_btag_raw = _empty_nested_array(len(events))

    bjet_pts = _pad_first_axis(bjet_pt_raw, 3)
    bjet_etas = _pad_first_axis(bjet_eta_raw, 3)
    bjet_btags = _pad_first_axis(bjet_btag_raw, 3)
    ht_bjets = np.asarray(ak.to_numpy(ak.sum(bjet_pt_raw, axis=1, mask_identity=False)), dtype=np.float32)
    n_bjets = np.asarray(ak.to_numpy(ak.num(bjet_pt_raw, axis=1)), dtype=np.float32)
    bjet_sum_btag = np.asarray(ak.to_numpy(ak.sum(bjet_btag_raw, axis=1, mask_identity=False)), dtype=np.float32)

    feature_values = OrderedDict([
        ("ht", ht),
        ("n_jets", n_jets),
        ("jet1_pt", jet_pts[0]),
        ("jet2_pt", jet_pts[1]),
        ("jet1_eta", jet_etas[0]),
        ("jet2_eta", jet_etas[1]),
        ("jet1_btag", jet_btags[0]),
        ("jet2_btag", jet_btags[1]),
        ("muon1_pt", muon_pt),
        ("electron1_pt", electron_pt),
        ("muon1_eta", muon_eta),
        ("electron1_eta", electron_eta),
        ("n_muons", n_muons),
        ("n_electrons", n_electrons),
        ("met_pt", _resolve_field(events, "MET.pt")),
        ("met_phi", _resolve_field(events, "MET.phi")),
        ("m_x", _resolve_field(events, "m_X")),
        ("m_h", _resolve_field(events, "m_H")),
        ("m_x_minus_m_h", _resolve_field(events, "m_X") - _resolve_field(events, "m_H")),
        ("m_x_minus_m_tt", _resolve_field(events, "m_X") - _resolve_field(events, "m_tt")),
        ("m_tt", _resolve_field(events, "m_tt")),
        ("tt_pt", _resolve_field(events, "tt_pt")),
        ("top_pt", _resolve_field(events, "top_pt")),
        ("top_mass", _resolve_field(events, "top_mass")),
        ("m_lead_b", _resolve_field(events, "m_lead_b")),
        ("lead_b_pt", _resolve_field(events, "lead_b_pt")),
        ("delta_r_jj", _resolve_field(events, "deltaR_jj")),
        ("delta_r_qq", _resolve_field(events, "deltaR_qq")),
        ("delta_r_bb", _resolve_field(events, "deltaR_bb")),
        ("ht_additional", ht_additional),
        ("total_btag", total_btag),
        ("n_additional_jets", n_additional_jets),
        ("jet3_pt", jet_pts[2]),
        ("jet4_pt", jet_pts[3]),
        ("jet5_pt", jet_pts[4]),
        ("jet6_pt", jet_pts[5]),
        ("jet3_eta", jet_etas[2]),
        ("jet4_eta", jet_etas[3]),
        ("jet5_eta", jet_etas[4]),
        ("jet6_eta", jet_etas[5]),
        ("jet3_btag", jet_btags[2]),
        ("jet4_btag", jet_btags[3]),
        ("jet5_btag", jet_btags[4]),
        ("jet6_btag", jet_btags[5]),
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

    available_names = tuple(feature_values.keys())
    selected_names = _select_feature_names(feature_set, available_names, BINARY_FEATURE_SETS)
    features = np.column_stack([feature_values[name] for name in selected_names]).astype(np.float32, copy=False)

    if "normalization_weight" in events.fields:
        weights = np.asarray(ak.to_numpy(events.normalization_weight), dtype=np.float32).reshape(-1)
    else:
        weights = np.ones(len(events), dtype=np.float32)

    return features, weights, selected_names


def build_pnn_feature_matrix(
    events: ak.Array,
    feature_set: str | Sequence[str] = "legacy",
) -> tuple[np.ndarray, np.ndarray, tuple[str, ...]]:
    if len(events) == 0:
        return np.empty((0, 0), dtype=np.float32), np.empty((0,), dtype=np.float32), tuple()

    jets = events.Jet
    jet_pts = _pad_first_axis(jets.pt, 6)
    jet_etas = _pad_first_axis(jets.eta, 6)
    jet_btags = _pad_first_axis(jets.btagDeepFlavB, 6)

    ht = np.asarray(ak.to_numpy(ak.sum(jets.pt, axis=1, mask_identity=False)), dtype=np.float32)
    n_jets = np.asarray(ak.to_numpy(ak.num(jets.pt, axis=1)), dtype=np.float32)
    total_btag = np.asarray(ak.to_numpy(ak.sum(jets.btagDeepFlavB, axis=1, mask_identity=False)), dtype=np.float32)
    ht_additional = np.maximum(ht - jet_pts[0] - jet_pts[1], 0.0).astype(np.float32, copy=False)
    n_additional_jets = np.maximum(n_jets - 2, 0).astype(np.float32, copy=False)

    if "Muon" in events.fields:
        mu_pt_raw = events.Muon.pt
        mu_eta_raw = events.Muon.eta
    else:
        mu_pt_raw = _empty_nested_array(len(events))
        mu_eta_raw = _empty_nested_array(len(events))

    if "Electron" in events.fields:
        el_pt_raw = events.Electron.pt
        el_eta_raw = events.Electron.eta
    else:
        el_pt_raw = _empty_nested_array(len(events))
        el_eta_raw = _empty_nested_array(len(events))

    muon_pt = _pad_first_axis(mu_pt_raw, 1)[0]
    muon_eta = _pad_first_axis(mu_eta_raw, 1)[0]
    electron_pt = _pad_first_axis(el_pt_raw, 1)[0]
    electron_eta = _pad_first_axis(el_eta_raw, 1)[0]
    n_muons = np.asarray(ak.to_numpy(ak.num(mu_pt_raw, axis=1)), dtype=np.float32)
    n_electrons = np.asarray(ak.to_numpy(ak.num(el_pt_raw, axis=1)), dtype=np.float32)

    if "Bjet" in events.fields:
        bjet_collection = events.Bjet
        bjet_fields = set(getattr(bjet_collection, "fields", []))
        bjet_pt_raw = bjet_collection.pt if "pt" in bjet_fields else _empty_nested_array(len(events))
        bjet_eta_raw = bjet_collection.eta if "eta" in bjet_fields else _empty_nested_array(len(events))
        if "btagDeepFlavB" in bjet_fields:
            bjet_btag_raw = bjet_collection.btagDeepFlavB
        else:
            bjet_btag_raw = _empty_nested_array(len(events))
    else:
        bjet_pt_raw = _empty_nested_array(len(events))
        bjet_eta_raw = _empty_nested_array(len(events))
        bjet_btag_raw = _empty_nested_array(len(events))

    bjet_pts = _pad_first_axis(bjet_pt_raw, 3)
    bjet_etas = _pad_first_axis(bjet_eta_raw, 3)
    bjet_btags = _pad_first_axis(bjet_btag_raw, 3)
    ht_bjets = np.asarray(ak.to_numpy(ak.sum(bjet_pt_raw, axis=1, mask_identity=False)), dtype=np.float32)
    n_bjets = np.asarray(ak.to_numpy(ak.num(bjet_pt_raw, axis=1)), dtype=np.float32)
    bjet_sum_btag = np.asarray(ak.to_numpy(ak.sum(bjet_btag_raw, axis=1, mask_identity=False)), dtype=np.float32)

    feature_values = OrderedDict([
        ("ht", ht),
        ("n_jets", n_jets),
        ("jet1_pt", jet_pts[0]),
        ("jet2_pt", jet_pts[1]),
        ("jet1_eta", jet_etas[0]),
        ("jet2_eta", jet_etas[1]),
        ("jet1_btag", jet_btags[0]),
        ("jet2_btag", jet_btags[1]),
        ("muon1_pt", muon_pt),
        ("electron1_pt", electron_pt),
        ("muon1_eta", muon_eta),
        ("electron1_eta", electron_eta),
        ("n_muons", n_muons),
        ("n_electrons", n_electrons),
        ("met_pt", _resolve_field(events, "MET.pt")),
        ("met_phi", _resolve_field(events, "MET.phi")),
        ("m_x", _resolve_field(events, "m_X")),
        ("m_h", _resolve_field(events, "m_H")),
        ("m_x_minus_m_h", _resolve_field(events, "m_X") - _resolve_field(events, "m_H")),
        ("m_x_minus_m_tt", _resolve_field(events, "m_X") - _resolve_field(events, "m_tt")),
        ("m_tt", _resolve_field(events, "m_tt")),
        ("tt_pt", _resolve_field(events, "tt_pt")),
        ("top_pt", _resolve_field(events, "top_pt")),
        ("top_mass", _resolve_field(events, "top_mass")),
        ("m_lead_b", _resolve_field(events, "m_lead_b")),
        ("lead_b_pt", _resolve_field(events, "lead_b_pt")),
        ("delta_r_jj", _resolve_field(events, "deltaR_jj")),
        ("delta_r_qq", _resolve_field(events, "deltaR_qq")),
        ("delta_r_bb", _resolve_field(events, "deltaR_bb")),
        ("whad_mass", _resolve_field(events, "whad_mass")),
        ("mlnu", _resolve_field(events, "mlnu")),
        ("mtlnu", _resolve_field(events, "mtlnu")),
        ("wboson.pt", _resolve_field(events, "wboson.pt")),
        ("tt_bar_mass", _resolve_field(events, "tt_bar_mass")),
        ("tt_bar_pt", _resolve_field(events, "tt_bar_pt")),
        ("whad_pt", _resolve_field(events, "whad_pt")),
        ("whad_eta", _resolve_field(events, "whad_eta")),
        ("whad_phi", _resolve_field(events, "whad_phi")),
        ("ht_additional", ht_additional),
        ("total_btag", total_btag),
        ("n_additional_jets", n_additional_jets),
        ("jet3_pt", jet_pts[2]),
        ("jet4_pt", jet_pts[3]),
        ("jet5_pt", jet_pts[4]),
        ("jet6_pt", jet_pts[5]),
        ("jet3_eta", jet_etas[2]),
        ("jet4_eta", jet_etas[3]),
        ("jet5_eta", jet_etas[4]),
        ("jet6_eta", jet_etas[5]),
        ("jet3_btag", jet_btags[2]),
        ("jet4_btag", jet_btags[3]),
        ("jet5_btag", jet_btags[4]),
        ("jet6_btag", jet_btags[5]),
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

    available_names = tuple(feature_values.keys())
    selected_names = _select_feature_names(feature_set, available_names, PNN_FEATURE_SETS)
    features = np.column_stack([feature_values[name] for name in selected_names]).astype(np.float32, copy=False)

    if "normalization_weight" in events.fields:
        weights = np.asarray(ak.to_numpy(events.normalization_weight), dtype=np.float32).reshape(-1)
    else:
        weights = np.ones(len(events), dtype=np.float32)

    return features, weights, selected_names


def append_mass_features(
    features: np.ndarray,
    mass_x: int | float,
    mass_y: int | float,
) -> np.ndarray:
    mass_columns = np.column_stack([
        np.full(len(features), mass_x, dtype=np.float32),
        np.full(len(features), mass_y, dtype=np.float32),
    ])
    return np.concatenate([features, mass_columns], axis=1)


def _weighted_mean(values: np.ndarray, weights: np.ndarray | None) -> float:
    values = np.asarray(values, dtype=np.float64).reshape(-1)
    if values.size == 0:
        return 0.0
    if weights is None:
        return float(np.mean(values))

    weights = np.asarray(weights, dtype=np.float64).reshape(-1)
    weight_sum = float(np.sum(weights))
    if weight_sum <= 0.0:
        return float(np.mean(values))
    return float(np.sum(values * weights) / weight_sum)


def weighted_binary_crossentropy(
    y_true: np.ndarray,
    y_score: np.ndarray,
    sample_weight: np.ndarray | None = None,
    epsilon: float = 1.0e-7,
) -> float:
    y_true = np.asarray(y_true, dtype=np.float64).reshape(-1)
    y_score = np.asarray(y_score, dtype=np.float64).reshape(-1)
    y_score = np.clip(y_score, epsilon, 1.0 - epsilon)
    losses = -(y_true * np.log(y_score) + (1.0 - y_true) * np.log(1.0 - y_score))
    return _weighted_mean(losses, sample_weight)


def weighted_accuracy(
    y_true: np.ndarray,
    y_score: np.ndarray,
    sample_weight: np.ndarray | None = None,
    threshold: float = 0.5,
) -> float:
    y_true = np.asarray(y_true, dtype=np.float64).reshape(-1)
    y_score = np.asarray(y_score, dtype=np.float64).reshape(-1)
    predicted = (y_score >= threshold).astype(np.float64)
    correct = (predicted == y_true).astype(np.float64)
    return _weighted_mean(correct, sample_weight)


def _subsample_indices(n_events: int, max_events: int | None, seed: int | None) -> np.ndarray:
    if max_events is None:
        return np.arange(n_events)
    max_events = int(max_events)
    if max_events <= 0 or n_events <= max_events:
        return np.arange(n_events)
    rng = np.random.default_rng(seed)
    return np.asarray(rng.choice(n_events, size=max_events, replace=False), dtype=np.int64)


def _import_shap() -> Any:
    module = importlib.import_module("shap")
    return module


def _normalize_shap_values(raw_values: Any) -> np.ndarray:
    if isinstance(raw_values, list):
        if len(raw_values) != 1:
            raise RuntimeError(
                f"Expected a single SHAP output for binary classification, got {len(raw_values)} outputs."
            )
        raw_values = raw_values[0]

    values = np.asarray(raw_values, dtype=np.float32)
    if values.ndim == 3 and values.shape[-1] == 1:
        values = values[..., 0]
    if values.ndim != 2:
        raise RuntimeError(f"Expected 2D SHAP values, got shape {values.shape}")
    return values


def compute_shap_feature_impacts(
    model: Any,
    inputs: np.ndarray,
    labels: np.ndarray,
    weights: np.ndarray,
    feature_names: Sequence[str],
    *,
    max_events: int | None = 1000,
    background_size: int = 200,
    kernel_nsamples: int | str = "auto",
    seed: int | None = None,
) -> dict[str, Any]:
    inputs = np.asarray(inputs, dtype=np.float32)
    labels = np.asarray(labels, dtype=np.float32).reshape(-1)
    weights = np.asarray(weights, dtype=np.float32).reshape(-1)

    if inputs.ndim != 2:
        raise ValueError(f"Expected 2D inputs, got shape {inputs.shape}")
    if len(inputs) == 0:
        raise ValueError("Cannot compute SHAP values on an empty sample.")
    if inputs.shape[1] != len(feature_names):
        raise ValueError(
            f"Feature-name count ({len(feature_names)}) does not match input width ({inputs.shape[1]})."
        )

    if seed is not None:
        seed = int(seed)
    rng = np.random.default_rng(seed)

    eval_indices = _subsample_indices(len(inputs), max_events=max_events, seed=seed)
    eval_inputs = inputs[eval_indices]
    eval_labels = labels[eval_indices]
    eval_weights = weights[eval_indices]

    background_size = max(int(background_size), 1)
    if len(inputs) <= background_size:
        background_inputs = inputs
    else:
        background_indices = np.asarray(rng.choice(len(inputs), size=background_size, replace=False), dtype=np.int64)
        background_inputs = inputs[background_indices]

    try:
        shap = _import_shap()
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "The 'shap' package is required for SHAP plots. Install it in the active environment "
            "and rerun with '--method shap'."
        ) from exc

    def predict_fn(batch: np.ndarray) -> np.ndarray:
        scores = np.asarray(model.predict(np.asarray(batch, dtype=np.float32), verbose=0), dtype=np.float32)
        return scores.reshape(-1, 1)

    explainer = shap.KernelExplainer(predict_fn, background_inputs)
    shap_values = _normalize_shap_values(explainer.shap_values(eval_inputs, nsamples=kernel_nsamples))

    expected_value = explainer.expected_value
    if isinstance(expected_value, list):
        if len(expected_value) != 1:
            raise RuntimeError(
                f"Expected a single SHAP expected value for binary classification, got {len(expected_value)} values."
            )
        expected_value = expected_value[0]
    expected_value = float(np.asarray(expected_value, dtype=np.float32).reshape(-1)[0])

    mean_abs = np.mean(np.abs(shap_values), axis=0)
    mean_signed = np.mean(shap_values, axis=0)
    impacts = []
    for feature_name, mean_abs_value, mean_signed_value in zip(feature_names, mean_abs, mean_signed):
        impacts.append({
            "feature": str(feature_name),
            "mean_abs_shap": float(mean_abs_value),
            "mean_shap": float(mean_signed_value),
        })
    impacts.sort(key=lambda item: item["mean_abs_shap"], reverse=True)

    scores = np.asarray(model.predict(eval_inputs, verbose=0), dtype=np.float32).reshape(-1)
    return {
        "sample_size": int(len(eval_inputs)),
        "background_size": int(len(background_inputs)),
        "expected_value": expected_value,
        "baseline": {
            "loss": weighted_binary_crossentropy(eval_labels, scores, sample_weight=eval_weights),
            "accuracy": weighted_accuracy(eval_labels, scores, sample_weight=eval_weights),
        },
        "impacts": impacts,
        "feature_values": eval_inputs.tolist(),
        "shap_values": shap_values.tolist(),
        "labels": eval_labels.tolist(),
        "weights": eval_weights.tolist(),
    }


def save_shap_summary_plots(
    payload: dict[str, Any],
    output_dir: str | Path,
    *,
    base_name: str = "shap_values",
    top_n: int = 20,
) -> dict[str, str]:
    import matplotlib.pyplot as plt

    output_dir = Path(output_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    top_n = max(int(top_n), 1)

    feature_names = list(payload.get("feature_names", []))
    feature_values = np.asarray(payload.get("feature_values", []), dtype=float)
    shap_values = np.asarray(payload.get("shap_values", []), dtype=float)
    impacts = list(payload.get("impacts", []))

    if not feature_names or feature_values.size == 0 or shap_values.size == 0:
        raise ValueError("SHAP payload is missing feature_names, feature_values, or shap_values.")
    if feature_values.shape != shap_values.shape:
        raise ValueError(
            f"feature_values shape {feature_values.shape} does not match shap_values shape {shap_values.shape}"
        )

    if impacts:
        ranking = [str(item["feature"]) for item in impacts]
    else:
        mean_abs = np.mean(np.abs(shap_values), axis=0)
        ranking = [feature_names[idx] for idx in np.argsort(mean_abs)[::-1]]

    selected = ranking[:top_n]
    selected_indices = [feature_names.index(name) for name in selected]

    bar_labels = list(reversed(selected))
    bar_values = [
        float(np.mean(np.abs(shap_values[:, feature_names.index(name)])))
        for name in bar_labels
    ]

    fig_height = max(4.5, 0.35 * len(bar_labels) + 1.6)
    fig, ax = plt.subplots(figsize=(10.5, fig_height))
    bars = ax.barh(bar_labels, bar_values, color="#b45309", alpha=0.9)
    ax.set_xlabel("mean(|SHAP value|)")
    ax.set_ylabel("Feature")
    ax.set_title("SHAP feature importance")
    ax.grid(axis="x", alpha=0.25)

    for bar, value in zip(bars, bar_values):
        ax.text(
            bar.get_width(),
            bar.get_y() + bar.get_height() / 2.0,
            f" {value:.4f}",
            va="center",
            ha="left",
            fontsize=9,
        )

    bar_path = output_dir / f"{base_name}_bar.png"
    fig.tight_layout()
    fig.savefig(bar_path, dpi=160, bbox_inches="tight")
    plt.close(fig)

    fig_height = max(5.5, 0.42 * len(selected) + 1.8)
    fig, ax = plt.subplots(figsize=(11.5, fig_height))
    rng = np.random.default_rng(12345)
    scatter = None
    for row_idx, feature_idx in enumerate(reversed(selected_indices)):
        values = shap_values[:, feature_idx]
        colors = feature_values[:, feature_idx]
        y_base = np.full(values.shape, row_idx, dtype=float)
        jitter = rng.uniform(-0.28, 0.28, size=values.shape)
        scatter = ax.scatter(
            values,
            y_base + jitter,
            c=colors,
            cmap="coolwarm",
            s=14,
            alpha=0.65,
            edgecolors="none",
            rasterized=True,
        )

    ax.axvline(0.0, color="#475569", linewidth=1.0, alpha=0.7)
    ax.set_yticks(np.arange(len(selected)))
    ax.set_yticklabels(list(reversed(selected)))
    ax.set_xlabel("SHAP value")
    ax.set_ylabel("Feature")
    ax.set_title("SHAP summary")
    ax.grid(axis="x", alpha=0.2)
    if scatter is not None:
        cbar = fig.colorbar(scatter, ax=ax, pad=0.02)
        cbar.set_label("Feature value")

    summary_path = output_dir / f"{base_name}_summary.png"
    fig.tight_layout()
    fig.savefig(summary_path, dpi=180, bbox_inches="tight")
    plt.close(fig)

    return {
        "bar_plot": str(bar_path),
        "summary_plot": str(summary_path),
    }


def write_shap_payload(
    payload: dict[str, Any],
    output_path: str | Path,
) -> str:
    output_path = Path(output_path).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    return str(output_path)
