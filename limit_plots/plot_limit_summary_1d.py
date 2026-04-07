#!/usr/bin/env python3
"""Create 1D plots from limit summary CSV files."""

from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt

try:  # pragma: no cover - optional dependency
    import mplhep

    mplhep.style.use(mplhep.style.CMS)
    _HAVE_MPLHEP = True
except ImportError:  # pragma: no cover - graceful fallback
    plt.style.use("seaborn-v0_8-darkgrid")
    _HAVE_MPLHEP = False


@dataclass(frozen=True)
class LimitSummaryPoint:
    mass_x: int
    mass_y: int
    expected_m2sigma: float
    expected_m1sigma: float
    expected: float
    expected_p1sigma: float
    expected_p2sigma: float

    @property
    def mass_label(self) -> str:
        return f"x{self.mass_x} y{self.mass_y}"

    @property
    def tick_label(self) -> str:
        return f"mX={self.mass_x}\nmY={self.mass_y}"


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return slug or "plot"


def read_limit_summary(path: Path) -> list[LimitSummaryPoint]:
    points: list[LimitSummaryPoint] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            points.append(
                LimitSummaryPoint(
                    mass_x=int(row["mass_x"]),
                    mass_y=int(row["mass_y"]),
                    expected_m2sigma=float(row["expected_m2sigma"]),
                    expected_m1sigma=float(row["expected_m1sigma"]),
                    expected=float(row["expected"]),
                    expected_p1sigma=float(row["expected_p1sigma"]),
                    expected_p2sigma=float(row["expected_p2sigma"]),
                )
            )
    return sorted(points, key=lambda point: (point.mass_x, point.mass_y))


def apply_label(ax: plt.Axes, label: str) -> None:
    ax.text(
        0.0,
        1.02,
        "Private work (CMS simulation)",
        transform=ax.transAxes,
        ha="left",
        va="bottom",
        fontsize=12,
        fontweight="bold",
    )
    ax.text(
        1.0,
        1.02,
        label,
        transform=ax.transAxes,
        ha="right",
        va="bottom",
        fontsize=11,
    )


def style_axes(ax: plt.Axes) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_linewidth(1.1)
    ax.spines["bottom"].set_linewidth(1.1)
    ax.tick_params(axis="x", which="major", length=0, pad=10, labelsize=10)
    ax.tick_params(axis="y", which="major", labelsize=11)
    ax.grid(True, axis="y", linestyle="--", linewidth=0.7, alpha=0.35)
    ax.grid(False, axis="x")


def annotate_points(ax: plt.Axes, x_positions: list[int], y_values: list[float]) -> None:
    for xpos, ypos in zip(x_positions, y_values):
        ax.annotate(
            f"{ypos:.3f}",
            (xpos, ypos),
            xytext=(0, 8),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=9,
            color="#1f2937",
        )


def draw_expected_bands(
    ax: plt.Axes,
    x_positions: list[int],
    band_2_low: list[float],
    band_2_high: list[float],
    band_1_low: list[float],
    band_1_high: list[float],
    *,
    color_2sigma: str,
    color_1sigma: str,
    alpha_2sigma: float,
    alpha_1sigma: float,
    label_bands: bool = True,
) -> None:
    label_2sigma = r"Expected $\pm 2\sigma$" if label_bands else None
    label_1sigma = r"Expected $\pm 1\sigma$" if label_bands else None
    ax.fill_between(
        x_positions,
        band_2_low,
        band_2_high,
        color=color_2sigma,
        alpha=alpha_2sigma,
        label=label_2sigma,
        zorder=1,
    )
    ax.fill_between(
        x_positions,
        band_1_low,
        band_1_high,
        color=color_1sigma,
        alpha=alpha_1sigma,
        label=label_1sigma,
        zorder=2,
    )
    # Outline the 2 sigma envelope so narrow bands remain visible.
    ax.plot(x_positions, band_2_low, color=color_2sigma, linewidth=1.0, alpha=0.95, zorder=3)
    ax.plot(x_positions, band_2_high, color=color_2sigma, linewidth=1.0, alpha=0.95, zorder=3)


def make_plot(points: list[LimitSummaryPoint], label: str, output_path: Path) -> None:
    x_positions = list(range(len(points)))
    x_labels = [point.tick_label for point in points]
    band_2_low = [point.expected_m2sigma for point in points]
    band_1_low = [point.expected_m1sigma for point in points]
    y_values = [point.expected for point in points]
    band_1_high = [point.expected_p1sigma for point in points]
    band_2_high = [point.expected_p2sigma for point in points]

    fig, ax = plt.subplots(figsize=(13.5, 7.2))
    draw_expected_bands(
        ax,
        x_positions,
        band_2_low,
        band_2_high,
        band_1_low,
        band_1_high,
        color_2sigma="#f4d35e",
        color_1sigma="#7fbf7b",
        alpha_2sigma=0.50,
        alpha_1sigma=0.58,
    )
    ax.plot(
        x_positions,
        y_values,
        marker="o",
        linewidth=2.6,
        markersize=7,
        color="#004c99",
        markerfacecolor="white",
        markeredgewidth=1.8,
        label="Median expected",
    )

    ax.set_xticks(x_positions)
    ax.set_xticklabels(x_labels)
    ax.set_xlim(-0.45, len(points) - 0.55)
    ax.set_ylim(0.0, max(band_2_high) * 1.15)
    ax.set_xlabel("Signal mass point", fontsize=12, labelpad=12)
    ax.set_ylabel("Median expected 95% CL upper limit", fontsize=12, labelpad=12)
    ax.set_title(f"{label} discriminator", fontsize=15, pad=18, fontweight="bold")
    style_axes(ax)
    annotate_points(ax, x_positions, y_values)
    ax.legend(frameon=False, fontsize=11, loc="upper right")
    apply_label(ax, label)
    fig.subplots_adjust(left=0.10, right=0.98, top=0.88, bottom=0.22)

    for ext in ("png", "pdf"):
        fig.savefig(output_path.with_suffix(f".{ext}"), bbox_inches="tight", dpi=300 if ext == "png" else None)
    plt.close(fig)


def make_overlay_plot(
    labelled_points: list[tuple[str, list[LimitSummaryPoint]]],
    output_path: Path,
) -> None:
    common_labels = [
        point.mass_label
        for point in labelled_points[0][1]
    ]
    common_positions = list(range(len(common_labels)))
    tick_labels = [point.tick_label for point in labelled_points[0][1]]

    fig, ax = plt.subplots(figsize=(13.5, 7.2))
    colors = ["#004c99", "#bb133e", "#2a9d8f", "#8d6cab"]
    band_1_colors = ["#93c5fd", "#fca5a5", "#99f6e4", "#d8b4fe"]
    band_2_colors = ["#dbeafe", "#fee2e2", "#ccfbf1", "#ede9fe"]
    for index, (label, points) in enumerate(labelled_points):
        values_m2 = {point.mass_label: point.expected_m2sigma for point in points}
        values_m1 = {point.mass_label: point.expected_m1sigma for point in points}
        values_by_label = {point.mass_label: point.expected for point in points}
        values_p1 = {point.mass_label: point.expected_p1sigma for point in points}
        values_p2 = {point.mass_label: point.expected_p2sigma for point in points}
        y_m2 = [values_m2[mass_label] for mass_label in common_labels]
        y_m1 = [values_m1[mass_label] for mass_label in common_labels]
        y_values = [values_by_label[mass_label] for mass_label in common_labels]
        y_p1 = [values_p1[mass_label] for mass_label in common_labels]
        y_p2 = [values_p2[mass_label] for mass_label in common_labels]
        draw_expected_bands(
            ax,
            common_positions,
            y_m2,
            y_p2,
            y_m1,
            y_p1,
            color_2sigma=band_2_colors[index % len(band_2_colors)],
            color_1sigma=band_1_colors[index % len(band_1_colors)],
            alpha_2sigma=0.40,
            alpha_1sigma=0.34,
            label_bands=False,
        )
        ax.plot(
            common_positions,
            y_values,
            marker="o",
            linewidth=2.6,
            markersize=7,
            label=label,
            color=colors[index % len(colors)],
            markerfacecolor="white",
            markeredgewidth=1.6,
        )

    ax.set_xticks(common_positions)
    ax.set_xticklabels(tick_labels)
    all_values = [
        point.expected_p2sigma
        for _, points in labelled_points
        for point in points
    ]
    ax.set_xlim(-0.45, len(common_labels) - 0.55)
    ax.set_ylim(0.0, max(all_values) * 1.28)
    ax.set_xlabel("Signal mass point", fontsize=12, labelpad=12)
    ax.set_ylabel("Median expected 95% CL upper limit", fontsize=12, labelpad=12)
    ax.set_title("Median expected limit comparison", fontsize=15, pad=18, fontweight="bold")
    style_axes(ax)
    ax.legend(frameon=False, fontsize=11, title="Model")
    apply_label(ax, "Median expected limits")
    fig.subplots_adjust(left=0.10, right=0.98, top=0.88, bottom=0.22)

    for ext in ("png", "pdf"):
        fig.savefig(output_path.with_suffix(f".{ext}"), bbox_inches="tight", dpi=300 if ext == "png" else None)
    plt.close(fig)


def parse_input_arg(raw: str) -> tuple[str, Path]:
    if "=" not in raw:
        raise argparse.ArgumentTypeError(
            "Each --input must be in the form <label>=<path/to/limit_summary.csv>"
        )
    label, path_str = raw.split("=", 1)
    label = label.strip()
    path = Path(path_str.strip()).expanduser().resolve()
    if not label:
        raise argparse.ArgumentTypeError("Input label must not be empty")
    return label, path


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot median expected limits from CSV summaries.")
    parser.add_argument(
        "--input",
        action="append",
        type=parse_input_arg,
        required=True,
        help="Input in the form <label>=<path/to/limit_summary.csv>. Can be provided multiple times.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("limit_plots/output/median_expected_1d"),
        help="Directory where plots will be written.",
    )
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    labelled_points: list[tuple[str, list[LimitSummaryPoint]]] = []
    for label, path in args.input:
        points = read_limit_summary(path)
        if not points:
            raise SystemExit(f"No rows found in {path}")
        make_plot(points, label, output_dir / f"{slugify(label)}_median_expected_1d")
        labelled_points.append((label, points))

    if len(labelled_points) > 1:
        baseline_labels = [point.mass_label for point in labelled_points[0][1]]
        if all([point.mass_label for point in points] == baseline_labels for _, points in labelled_points[1:]):
            make_overlay_plot(labelled_points, output_dir / "comparison_median_expected_1d")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
