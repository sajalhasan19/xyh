#!/usr/bin/env python3
"""Plot limits per category from Combine AsymptoticLimits logs."""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import List, Optional, Tuple


PAT_EXPECTED = re.compile(r"Expected\s+50\.0%:\s+r\s*<\s*([0-9.eE+-]+)")
PAT_OBSERVED = re.compile(r"Observed Limit:\s+r\s*<\s*([0-9.eE+-]+)")
PAT_MASS = re.compile(r"_(x[0-9]+_y[0-9]+|mH[0-9]+)(?:_|$)")


def parse_log(path: Path) -> Tuple[float, Optional[float]]:
    expected = None
    observed = None
    for line in path.read_text().splitlines():
        if expected is None:
            m = PAT_EXPECTED.search(line)
            if m:
                expected = float(m.group(1))
        if observed is None:
            m = PAT_OBSERVED.search(line)
            if m:
                observed = float(m.group(1))
        if expected is not None and observed is not None:
            break
    if expected is None:
        raise ValueError(f"{path}: missing median expected (50%) entry")
    return expected, observed


def extract_labels(path: Path, tag_hint: Optional[str]) -> Tuple[str, Optional[str]]:
    """Return (category, mass_point) derived from the filename."""
    stem = path.stem
    if not stem.startswith("AsymptoticLimits_"):
        return stem, None
    body = stem[len("AsymptoticLimits_") :]

    mass_point = None
    category = body

    mass_match = PAT_MASS.search(body)
    if mass_match:
        category = body[: mass_match.start()]
        mass_point = mass_match.group(1)
        tail = body[mass_match.end() :]
        if tag_hint and tail == tag_hint:
            pass  # tag stripped intentionally
    elif tag_hint and body.endswith(f"_{tag_hint}"):
        category = body[: -len(tag_hint) - 1]

    return category, mass_point


def collect_limits(log_dir: Path, tag: Optional[str]) -> List[Tuple[str, float, Optional[float]]]:
    logs = sorted(log_dir.glob("AsymptoticLimits_*.log"))
    if not logs:
        raise FileNotFoundError(f"No AsymptoticLimits_*.log files found in {log_dir}")

    results: List[Tuple[str, float, Optional[float]]] = []
    for log_path in logs:
        category, _ = extract_labels(log_path, tag)
        expected, observed = parse_log(log_path)
        results.append((category, expected, observed))
    return results


def plot_limits(
    entries: List[Tuple[str, float, Optional[float]]],
    output_base: Path,
    log_scale: bool = False,
) -> List[Path]:
    import matplotlib.pyplot as plt

    categories, exp_vals, obs_vals = zip(*entries)
    order = sorted(range(len(categories)), key=lambda i: exp_vals[i])
    categories = [categories[i] for i in order]
    exp_vals = [exp_vals[i] for i in order]
    obs_vals = [obs_vals[i] for i in order]

    fig, ax = plt.subplots(figsize=(9, 0.5 * len(categories) + 2))
    y_pos = list(range(len(categories)))

    ax.barh(y_pos, exp_vals, color="#004c99", alpha=0.85, label="Expected (50%)")

    obs_x = [v for v in obs_vals if v is not None]
    if obs_x:
        obs_y = [i for i, v in enumerate(obs_vals) if v is not None]
        ax.scatter(obs_x, obs_y, color="#bb133e", marker="s", s=36, label="Observed")

    ax.set_yticks(y_pos)
    ax.set_yticklabels(categories, fontsize=10)
    ax.set_xlabel(r"95% CL limit on $r$", ha="right", x=1.0)
    ax.invert_yaxis()
    if log_scale:
        ax.set_xscale("log")
    ax.grid(True, axis="x", linestyle="--", alpha=0.5)
    ax.legend(loc="lower right", frameon=False)
    fig.tight_layout()

    outputs: List[Path] = []
    for ext in ("png", "pdf"):
        out_path = output_base.with_suffix(f".{ext}")
        fig.savefig(out_path, dpi=300 if ext == "png" else None, bbox_inches="tight")
        outputs.append(out_path)
    plt.close(fig)
    return outputs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot per-category limits from Combine logs.")
    parser.add_argument("log_dir", type=Path, help="Directory containing AsymptoticLimits_*.log files")
    parser.add_argument("--tag", type=str, default=None, help="Tag suffix used in the filenames (e.g. RunIII)")
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output path prefix (default: <log_dir>/per_category_limits)",
    )
    parser.add_argument("--log-scale", action="store_true", help="Use log scale on the x axis")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    log_dir = args.log_dir.expanduser().resolve()
    entries = collect_limits(log_dir, args.tag)

    output_base = args.output or (log_dir / "per_category_limits")
    outputs = plot_limits(entries, output_base, log_scale=args.log_scale)
    print(f"[+] Processed {len(entries)} log files in {log_dir}")
    for out in outputs:
        print(f"[+] Wrote plot: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
