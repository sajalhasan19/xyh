#!/usr/bin/env python3
"""High-level entry point to plot Combine limits."""

from __future__ import annotations

import argparse
import csv
import importlib
import sys
from functools import lru_cache
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple

import yaml

try:
    if __package__ in (None, ""):
        # Support running as a script via `python limit_plots/plot_limits.py`.
        sys.path.append(str(Path(__file__).resolve().parent.parent))
        from limit_plots.limits import LimitCollection  # type: ignore  # noqa: E402
        from limit_plots.plotter import LimitPlotter  # type: ignore  # noqa: E402
    else:  # pragma: no cover - exercised when imported as package
        from .limits import LimitCollection  # type: ignore  # noqa: E402
        from .plotter import LimitPlotter  # type: ignore  # noqa: E402
except ModuleNotFoundError as exc:  # pragma: no cover - import guard
    if exc.name == "matplotlib":
        raise SystemExit(
            "matplotlib is required for plotting. Install it with 'pip install matplotlib mplhep'."
        ) from exc
    raise


def _ensure_sys_paths(paths: Iterable[Path]) -> None:
    for raw_path in paths:
        if not raw_path:
            continue
        resolved = raw_path.expanduser().resolve()
        path_str = str(resolved)
        if path_str not in sys.path:
            sys.path.append(path_str)


def build_per_signal_lookup(per_cfg: Dict[str, object]) -> Callable[[int, int], float]:
    """
    Create a callable that returns a scaling factor for each (mass_x, mass_y) combination based
    on information stored in a python module. This mirrors the approach used in the AZH setup
    where Combine r-modifiers are converted into σ×BR limits.
    """
    if not isinstance(per_cfg, dict):
        raise TypeError("scale.per_signal must be a mapping")

    pattern = per_cfg.get("pattern")
    module_name = per_cfg.get("module")
    sqrt_s = per_cfg.get("sqrt_s")

    if not pattern:
        raise ValueError("scale.per_signal requires a 'pattern' entry")
    if not module_name:
        raise ValueError("scale.per_signal requires a 'module' entry")
    if sqrt_s is None:
        raise ValueError("scale.per_signal requires a 'sqrt_s' entry")

    sqrt_s_candidates: Tuple[object, ...] = (
        sqrt_s,
        float(sqrt_s),
        int(float(sqrt_s)),
    )

    repo_root = Path(__file__).resolve().parent.parent
    default_paths = [
        repo_root,
        repo_root / "modules",
        repo_root / "modules/cmsdb",
    ]
    extra_paths = per_cfg.get("python_paths", [])
    extra_path_objs = [
        Path(p) if isinstance(p, str) else p
        for p in extra_paths
    ]
    _ensure_sys_paths(default_paths + extra_path_objs)

    module = importlib.import_module(module_name)
    field = per_cfg.get("field", "xsecs")
    field_key = per_cfg.get("field_key")
    value_attr = per_cfg.get("value_attr", "nominal")
    multiplier = float(per_cfg.get("multiplier", 1.0))
    default = per_cfg.get("default")

    @lru_cache(maxsize=None)
    def _lookup(mass_x: int, mass_y: int) -> float:
        attr_name = pattern.format(mass_x=int(mass_x), mass_y=int(mass_y))
        target = module
        try:
            for part in attr_name.split("."):
                if not part:
                    continue
                target = getattr(target, part)
        except AttributeError as exc:
            if default is not None:
                return float(default) * multiplier
            raise AttributeError(
                f"Attribute '{attr_name}' not found in module '{module_name}'"
            ) from exc

        value = target
        try:
            if field:
                value = getattr(value, field)
            if isinstance(value, dict):
                keys_to_try = []
                if field_key is not None:
                    keys_to_try.append(field_key)
                keys_to_try.extend(sqrt_s_candidates)
                for key in keys_to_try:
                    candidates = [key]
                    try:
                        candidates.append(float(key))
                    except Exception:
                        pass
                    try:
                        candidates.append(int(float(key)))
                    except Exception:
                        pass
                    matched = False
                    for candidate in candidates:
                        if candidate in value:
                            value = value[candidate]
                            matched = True
                            break
                    if matched:
                        break
                else:
                    raise KeyError(f"key matching sqrt_s={sqrt_s} not found in '{field}' dictionary")

            if value_attr:
                if hasattr(value, value_attr):
                    attr = getattr(value, value_attr)
                    value = attr() if callable(attr) else attr
            if hasattr(value, "nominal"):
                value = value.nominal
            result = float(value) * multiplier
        except Exception as exc:
            if default is not None:
                return float(default) * multiplier
            raise RuntimeError(
                f"Failed to derive scale value for mass point ({mass_x}, {mass_y})"
            ) from exc
        return result

    return _lookup


def read_signals(path: Path) -> List[str]:
    with path.open("r", encoding="utf-8") as handle:
        return [line.strip() for line in handle if line.strip() and not line.strip().startswith("#")]


def write_csv(output_path: Path, collection: LimitCollection) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "mass_x",
                "mass_y",
                "tag",
                "expected_m2sigma",
                "expected_m1sigma",
                "expected",
                "expected_p1sigma",
                "expected_p2sigma",
                "observed",
            ]
        )
        for row in collection.to_rows():
            writer.writerow(row)


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create limit plots from Combine logs.")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("limit_plots/config.yaml"),
        help="Path to configuration YAML file.",
    )
    parser.add_argument(
        "--combination",
        type=str,
        default=None,
        help="Name of the combination directory inside the output folder.",
    )
    parser.add_argument(
        "--tag",
        type=str,
        default=None,
        help="Tag suffix used in the log filenames (default: value from config).",
    )
    parser.add_argument(
        "--focus-mass-y",
        type=int,
        action="append",
        dest="focus_mass_y",
        help="Plot only the specified mY values (can be provided multiple times).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory where plots should be written (default: <outdir>/<combination>/plots).",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=None,
        help="Optional path to store a CSV summary of the limits.",
    )
    parser.add_argument(
        "--signals",
        type=Path,
        default=None,
        help="Optional alternative signal list file (one 'mX_mY' per line).",
    )
    parser.add_argument(
        "--scale-factor",
        type=float,
        default=None,
        help="Override scale factor applied to the limits (multiplies expected/observed values).",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def load_config(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Config file '{path}' not found")
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    cfg = load_config(args.config)

    base_outdir = Path(cfg.get("outdir", ".")).expanduser().resolve()
    combination = args.combination or cfg.get("combination")
    if not combination:
        raise ValueError("Combination name must be provided via --combination or config['combination']")

    tag = args.tag or cfg.get("tag") or cfg.get("comb_name")
    if not tag:
        raise ValueError("Tag must be provided via --tag or config['tag']")

    signals_path = args.signals or cfg.get("signals") or cfg.get("signals_file")
    signals: Optional[List[str]] = None
    if signals_path:
        signals = read_signals(Path(signals_path).expanduser().resolve())

    scale_mode = "multiply"
    scale_factor = args.scale_factor
    scale_lookup: Optional[Callable[[int, int], float]] = None
    if scale_factor is None:
        scale_cfg = cfg.get("scale", {})
        if isinstance(scale_cfg, dict):
            if "multiply_by" in scale_cfg:
                scale_factor = scale_cfg["multiply_by"]
                scale_mode = "multiply"
            elif "divide_by" in scale_cfg:
                scale_factor = scale_cfg["divide_by"]
                scale_mode = "divide"
            elif "factor" in scale_cfg:
                scale_factor = scale_cfg["factor"]
                scale_mode = str(scale_cfg.get("mode", "multiply")).lower()
            else:
                scale_factor = cfg.get("sigma_ref_pb", 1.0)
            if "per_signal" in scale_cfg:
                scale_lookup = build_per_signal_lookup(scale_cfg["per_signal"])
        elif scale_cfg:
            scale_factor = scale_cfg
        else:
            scale_factor = cfg.get("sigma_ref_pb", 1.0)
    else:
        scale_cfg = cfg.get("scale", {})
        if isinstance(scale_cfg, dict) and "per_signal" in scale_cfg:
            scale_lookup = build_per_signal_lookup(scale_cfg["per_signal"])
    if scale_factor is None:
        scale_factor = 1.0
    scale_factor = float(scale_factor)
    if scale_factor <= 0:
        raise ValueError("Scale factor must be positive")
    if scale_mode not in {"multiply", "divide"}:
        raise ValueError(f"Unsupported scale mode '{scale_mode}'")

    label = cfg.get("label", tag)
    sqrt_s = cfg.get("sqrt_s", "13.6 TeV")

    focus_mass_y = args.focus_mass_y or cfg.get("focus_mass_y")
    if isinstance(focus_mass_y, int):
        focus_mass_y = [focus_mass_y]

    combination_dir = base_outdir / combination
    if not combination_dir.is_dir():
        raise FileNotFoundError(f"Combination directory '{combination_dir}' not found")

    collection = LimitCollection(
        log_dir=combination_dir,
        tag=tag,
        signals=signals,
        scale_factor=scale_factor,
        scale_mode=scale_mode,
        scale_lookup=scale_lookup,
    )

    output_dir = args.output_dir or (combination_dir / "plots")
    output_dir.mkdir(parents=True, exist_ok=True)

    plotter = LimitPlotter(output_dir=output_dir, label=label, sqrt_s=sqrt_s)

    target_mass_y = focus_mass_y or collection.unique_mass_y()
    generated_plots: List[Path] = []

    for mass_y in sorted(set(target_mass_y)):
        xs, exp_values = collection.expected_curve(mass_y)
        if len(xs) >= 2:
            plots_expected = plotter.plot_expected(collection, mass_y)
            if plots_expected:
                generated_plots.extend(plots_expected)
            plots_brazil = plotter.plot_brazil(collection, mass_y)
            if plots_brazil:
                generated_plots.extend(plots_brazil)
        else:
            print(f"[i] Skipping mY={mass_y}: not enough points for a 1D plot.")

    unique_mx = {p.mass_x for p in collection.points}
    unique_my = {p.mass_y for p in collection.points}
    if len(collection.points) >= 3 and len(unique_mx) >= 2 and len(unique_my) >= 2:
        try:
            plots_heatmap = plotter.plot_heatmap(collection)
            if plots_heatmap:
                generated_plots.extend(plots_heatmap)
        except RuntimeError as exc:
            print(f"[i] Skipping heatmap: {exc}")
    else:
        print("[i] Not enough points for a 2D heatmap.")

    csv_path = args.csv or (output_dir / "limit_summary.csv")
    write_csv(csv_path, collection)

    print(f"[+] Processed {len(collection.points)} limit points from {combination_dir}")
    for path in generated_plots:
        if path:
            print(f"[+] Wrote plot: {path}")
    print(f"[+] Wrote CSV summary: {csv_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
