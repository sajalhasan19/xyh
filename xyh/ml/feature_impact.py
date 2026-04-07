"""
Helpers for post-training feature-impact studies on XYH ML models.
"""

from __future__ import annotations

from collections import OrderedDict
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


def append_mass_features(features: np.ndarray, mass_x: int | float, mass_y: int | float) -> np.ndarray:
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


def compute_permutation_feature_impacts(
    model: Any,
    inputs: np.ndarray,
    labels: np.ndarray,
    weights: np.ndarray,
    feature_names: Sequence[str],
    *,
    repeats: int = 3,
    max_events: int | None = 50000,
    seed: int | None = None,
) -> dict[str, Any]:
    inputs = np.asarray(inputs, dtype=np.float32)
    labels = np.asarray(labels, dtype=np.float32).reshape(-1)
    weights = np.asarray(weights, dtype=np.float32).reshape(-1)

    if inputs.ndim != 2:
        raise ValueError(f"Expected 2D inputs, got shape {inputs.shape}")
    if len(inputs) == 0:
        raise ValueError("Cannot compute feature impacts on an empty sample.")
    if inputs.shape[1] != len(feature_names):
        raise ValueError(
            f"Feature-name count ({len(feature_names)}) does not match input width ({inputs.shape[1]})."
        )

    if seed is not None:
        seed = int(seed)
    repeats = max(int(repeats), 1)
    indices = _subsample_indices(len(inputs), max_events=max_events, seed=seed)
    inputs = inputs[indices]
    labels = labels[indices]
    weights = weights[indices]

    rng = np.random.default_rng(seed)
    baseline_scores = np.asarray(model.predict(inputs, verbose=0), dtype=np.float32).reshape(-1)
    baseline_loss = weighted_binary_crossentropy(labels, baseline_scores, sample_weight=weights)
    baseline_accuracy = weighted_accuracy(labels, baseline_scores, sample_weight=weights)

    impacts = []
    for feature_idx, feature_name in enumerate(feature_names):
        loss_values = []
        accuracy_values = []
        for _ in range(repeats):
            shuffled_inputs = np.array(inputs, copy=True)
            shuffled_inputs[:, feature_idx] = rng.permutation(shuffled_inputs[:, feature_idx])
            shuffled_scores = np.asarray(model.predict(shuffled_inputs, verbose=0), dtype=np.float32).reshape(-1)
            loss_values.append(weighted_binary_crossentropy(labels, shuffled_scores, sample_weight=weights))
            accuracy_values.append(weighted_accuracy(labels, shuffled_scores, sample_weight=weights))

        mean_loss = float(np.mean(loss_values))
        mean_accuracy = float(np.mean(accuracy_values))
        impacts.append({
            "feature": str(feature_name),
            "loss": mean_loss,
            "loss_delta": mean_loss - baseline_loss,
            "accuracy": mean_accuracy,
            "accuracy_delta": baseline_accuracy - mean_accuracy,
            "repeats": repeats,
        })

    impacts.sort(key=lambda item: item["loss_delta"], reverse=True)

    return {
        "sample_size": int(len(inputs)),
        "repeats": repeats,
        "baseline": {
            "loss": baseline_loss,
            "accuracy": baseline_accuracy,
        },
        "impacts": impacts,
    }
