#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable
from typing import Any

import numpy as np
import tensorflow as tf

from xyh.ml.feature_impact import (
    compute_permutation_feature_impacts,
    load_parquet_inputs,
)
from xyh.ml.xyh_binary import XYHBinaryModel, XYHParameterizedBinaryModel
from xyh.ml.xyh_pnn import XYHPNNModel


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute post-training feature impacts from a saved model and labeled parquet samples.",
    )
    parser.add_argument(
        "--model-path",
        required=True,
        help="Path to a saved Keras model (.keras file or SavedModel directory).",
    )
    parser.add_argument(
        "--model-type",
        required=True,
        choices=("binary", "binary-parameterized", "pnn"),
        help="Model family used to build the feature matrix.",
    )
    parser.add_argument(
        "--feature-set",
        default="legacy",
        help="Feature set used in training (e.g. legacy, curated, all).",
    )
    parser.add_argument(
        "--signal-events",
        nargs="+",
        required=True,
        help="One or more parquet files representing the signal sample.",
    )
    parser.add_argument(
        "--background-events",
        nargs="+",
        required=True,
        help="One or more parquet files representing the background sample.",
    )
    parser.add_argument(
        "--eval-mass-x",
        type=float,
        help="Conditioning mass x used for parameterized inference inputs.",
    )
    parser.add_argument(
        "--eval-mass-y",
        type=float,
        help="Conditioning mass y used for parameterized inference inputs.",
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=3,
        help="Number of permutation repeats per feature.",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=50000,
        help="Maximum number of combined events used for the impact study after random subsampling.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for event subsampling and feature shuffling.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output JSON path. Defaults to a sibling file next to the model path.",
    )
    return parser.parse_args(argv)


def default_output_path(model_path: Path) -> Path:
    if model_path.is_dir():
        return model_path.parent / f"{model_path.name}_feature_impacts.json"
    stem = model_path.stem
    if not stem:
        stem = model_path.name
    return model_path.with_name(f"{stem}_feature_impacts.json")


class InferenceModelAdapter:
    def __init__(self, predictor: Any) -> None:
        self._predictor = predictor

    def predict(self, inputs: np.ndarray, verbose: int = 0) -> np.ndarray:
        del verbose
        outputs = self._predictor(inputs)
        return np.asarray(outputs, dtype=np.float32)


def _saved_model_predictor(model_path: Path):
    saved_model = tf.saved_model.load(str(model_path))
    signatures = getattr(saved_model, "signatures", {})
    if not signatures:
        raise RuntimeError(f"No callable signatures found in SavedModel: {model_path}")

    if "serving_default" in signatures:
        fn = signatures["serving_default"]
    elif len(signatures) == 1:
        fn = next(iter(signatures.values()))
    else:
        raise RuntimeError(
            f"SavedModel has multiple signatures ({', '.join(signatures.keys())}) and no 'serving_default'."
        )

    def predict(inputs: np.ndarray) -> np.ndarray:
        tensor_inputs = tf.convert_to_tensor(inputs, dtype=tf.float32)
        outputs = fn(tensor_inputs)
        if isinstance(outputs, dict):
            if len(outputs) != 1:
                raise RuntimeError(
                    f"SavedModel signature returned multiple outputs ({', '.join(outputs.keys())}); "
                    "automatic selection is ambiguous."
                )
            outputs = next(iter(outputs.values()))
        return outputs.numpy()

    return predict


def load_inference_model(model_path: Path) -> InferenceModelAdapter:
    if model_path.is_dir():
        return InferenceModelAdapter(_saved_model_predictor(model_path))

    return InferenceModelAdapter(tf.keras.models.load_model(model_path))


def build_inputs(
    args: argparse.Namespace,
    signal_events,
    background_events,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, tuple[str, ...]]:
    if args.model_type == "binary":
        model_stub = object.__new__(XYHBinaryModel)
        feature_source = XYHBinaryModel
    elif args.model_type == "binary-parameterized":
        model_stub = object.__new__(XYHParameterizedBinaryModel)
        feature_source = XYHParameterizedBinaryModel
    else:
        model_stub = object.__new__(XYHPNNModel)
        feature_source = XYHPNNModel

    model_stub.parameters = {"feature_set": args.feature_set}

    signal_x, signal_w = model_stub._build_feature_matrix(signal_events)
    background_x, background_w = model_stub._build_feature_matrix(background_events)

    feature_set_key = str(args.feature_set).strip().lower()
    if isinstance(args.feature_set, (list, tuple)):
        feature_names = tuple(args.feature_set)
    elif not feature_set_key or feature_set_key == "legacy":
        feature_names = tuple(feature_source.legacy_feature_names)
    elif feature_set_key == "curated":
        feature_names = tuple(feature_source.curated_feature_names)
    elif feature_set_key == "all":
        raise ValueError(
            "feature_set=all is not supported in the post-training helper yet. "
            "Use the exact named set used in training (e.g. legacy or curated)."
        )
    else:
        raise ValueError(
            f"Unsupported feature_set '{args.feature_set}' for post-training helper. "
            "Use the exact named set used in training."
        )

    if args.model_type in {"binary-parameterized", "pnn"}:
        if args.eval_mass_x is None or args.eval_mass_y is None:
            raise ValueError("--eval-mass-x and --eval-mass-y are required for parameterized models.")
        signal_x = model_stub._append_mass_features(signal_x, (int(args.eval_mass_x), int(args.eval_mass_y)))
        background_x = model_stub._append_mass_features(background_x, (int(args.eval_mass_x), int(args.eval_mass_y)))
        feature_names = tuple(feature_names) + ("mass_x", "mass_y")

    if signal_x.shape[1] != len(feature_names):
        raise RuntimeError(
            f"Feature-name count ({len(feature_names)}) does not match signal input width ({signal_x.shape[1]})."
        )
    if background_x.shape[1] != len(feature_names):
        raise RuntimeError(
            f"Feature-name count ({len(feature_names)}) does not match background input width ({background_x.shape[1]})."
        )

    x_all = np.concatenate([signal_x, background_x], axis=0)
    w_all = np.concatenate([signal_w, background_w], axis=0)
    y_all = np.concatenate([
        np.ones(len(signal_x), dtype=np.float32),
        np.zeros(len(background_x), dtype=np.float32),
    ])

    return x_all, y_all, w_all, feature_names


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)

    model_path = Path(args.model_path).expanduser()
    if not model_path.exists():
        raise FileNotFoundError(f"Model path not found: {model_path}")

    output_path = Path(args.output).expanduser() if args.output else default_output_path(model_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    signal_events = load_parquet_inputs(args.signal_events)
    background_events = load_parquet_inputs(args.background_events)
    if len(signal_events) == 0:
        raise RuntimeError("No signal events were loaded.")
    if len(background_events) == 0:
        raise RuntimeError("No background events were loaded.")

    model = load_inference_model(model_path)
    inputs, labels, weights, feature_names = build_inputs(args, signal_events, background_events)

    payload = compute_permutation_feature_impacts(
        model,
        inputs,
        labels,
        weights,
        feature_names,
        repeats=args.repeats,
        max_events=args.max_events,
        seed=args.seed,
    )
    payload.update({
        "model_path": str(model_path),
        "model_type": args.model_type,
        "feature_set": args.feature_set,
        "feature_names": list(feature_names),
        "signal_files": [str(Path(path).expanduser()) for path in args.signal_events],
        "background_files": [str(Path(path).expanduser()) for path in args.background_events],
        "source": "post_training",
        "signal_events_total": int(len(signal_events)),
        "background_events_total": int(len(background_events)),
    })
    if args.model_type in {"binary-parameterized", "pnn"}:
        payload["eval_mass"] = {
            "mass_x": float(args.eval_mass_x),
            "mass_y": float(args.eval_mass_y),
        }

    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    print(f"Saved feature impacts to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
