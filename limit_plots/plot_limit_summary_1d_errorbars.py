#!/usr/bin/env python3
"""Create 1D plots from limit summary CSV files using point error bars."""

from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt

PB_TO_FB = 1000.0
SCALAR_X_FB = 30.0
PSEUDOSCALAR_X_FB = 37.0


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
        return f"({self.mass_x}, {self.mass_y})"


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return slug or "plot"


def get_label_colors(label: str, fallback_index: int) -> tuple[str, str]:
    normalized = label.strip().lower()
    custom_colors = {
        "pnn": ("#dc2626", "#fca5a5"),
        "pnn (post-ttbb)": ("#5b21b6", "#c4b5fd"),
        "pnn (ttbb_v1)": ("#5b21b6", "#c4b5fd"),
    }
    if normalized in custom_colors:
        return custom_colors[normalized]

    colors = ["#0f766e", "#b91c1c", "#1d4ed8", "#7c3aed"]
    light_colors = ["#94a3b8", "#fca5a5", "#93c5fd", "#d8b4fe"]
    return colors[fallback_index % len(colors)], light_colors[fallback_index % len(light_colors)]


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


def apply_label(ax: plt.Axes, label: str, *, right_x: float = 1.0, right_ha: str = "right") -> None:
    ax.text(
        0.0,
        1.02,
        "Private work (CMS simulation)",
        transform=ax.transAxes,
        ha="left",
        va="bottom",
        fontsize=5,
        fontweight="bold",
    )
    ax.text(
        right_x,
        1.02,
        label,
        transform=ax.transAxes,
        ha=right_ha,
        va="bottom",
        fontsize=11,
    )


def style_axes(ax: plt.Axes) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_linewidth(1.1)
    ax.spines["bottom"].set_linewidth(1.1)
    ax.tick_params(axis="x", which="major", length=0, pad=10, labelsize=12)
    ax.tick_params(axis="y", which="major", labelsize=11)
    ax.grid(True, axis="y", linestyle="--", linewidth=0.7, alpha=0.35)
    ax.grid(False, axis="x")


def add_prediction_lines(ax: plt.Axes, mass_labels: list[str], x_positions: list[float]) -> None:
    start_label = "x800 y450"
    end_label = "x950 y400"
    if start_label not in mass_labels or end_label not in mass_labels:
        return

    x_by_label = dict(zip(mass_labels, x_positions))
    xmin = ax.get_xlim()[0]
    xmax = max(x_by_label[start_label], x_by_label[end_label]) + 0.18
    line_width = 3.8

    def half_band_from_linewidth(y_value: float) -> float:
        # Make the translucent band visibly thicker than the dashed line in screen space.
        y_disp = ax.transData.transform((0.0, y_value))[1]
        half_line_px = line_width * ax.figure.dpi / 72.0
        y_hi = ax.transData.inverted().transform((0.0, y_disp + half_line_px))[1]
        y_lo = ax.transData.inverted().transform((0.0, y_disp - half_line_px))[1]
        return max(abs(y_hi - y_value), abs(y_value - y_lo))

    scalar_half_band = half_band_from_linewidth(SCALAR_X_FB)
    pseudoscalar_half_band = half_band_from_linewidth(PSEUDOSCALAR_X_FB)

    ax.fill_between(
        [xmin, xmax],
        [SCALAR_X_FB - scalar_half_band, SCALAR_X_FB - scalar_half_band],
        [SCALAR_X_FB + scalar_half_band, SCALAR_X_FB + scalar_half_band],
        color="#ec4899",
        alpha=0.28,
        zorder=0,
    )
    ax.hlines(
        SCALAR_X_FB,
        xmin=xmin,
        xmax=xmax,
        color="#ec4899",
        linestyle="--",
        linewidth=line_width,
        alpha=0.9,
        zorder=1,
    )
    ax.fill_between(
        [xmin, xmax],
        [PSEUDOSCALAR_X_FB - pseudoscalar_half_band, PSEUDOSCALAR_X_FB - pseudoscalar_half_band],
        [PSEUDOSCALAR_X_FB + pseudoscalar_half_band, PSEUDOSCALAR_X_FB + pseudoscalar_half_band],
        color="#eab308",
        alpha=0.28,
        zorder=0,
    )
    ax.hlines(
        PSEUDOSCALAR_X_FB,
        xmin=xmin,
        xmax=xmax,
        color="#eab308",
        linestyle="--",
        linewidth=line_width,
        alpha=0.9,
        zorder=1,
    )


def annotate_points(ax: plt.Axes, x_positions: list[int], y_values: list[float]) -> None:
    for xpos, ypos in zip(x_positions, y_values):
        ax.annotate(
            f"{ypos:.3f}",
            (xpos, ypos),
            xytext=(0, 9),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=9,
            color="#1f2937",
        )


def draw_expected_errorbars(
    ax: plt.Axes,
    x_positions: list[float],
    points: list[LimitSummaryPoint],
    *,
    color: str,
    light_color: str,
    label: str | None = None,
) -> None:
    y_values = [point.expected * PB_TO_FB for point in points]
    err_1_low = [(point.expected - point.expected_m1sigma) * PB_TO_FB for point in points]
    err_1_high = [(point.expected_p1sigma - point.expected) * PB_TO_FB for point in points]
    err_2_low = [(point.expected - point.expected_m2sigma) * PB_TO_FB for point in points]
    err_2_high = [(point.expected_p2sigma - point.expected) * PB_TO_FB for point in points]

    ax.errorbar(
        x_positions,
        y_values,
        yerr=[err_2_low, err_2_high],
        fmt="none",
        ecolor=light_color,
        elinewidth=2.0,
        capsize=0,
        zorder=1,
    )
    ax.errorbar(
        x_positions,
        y_values,
        yerr=[err_1_low, err_1_high],
        fmt="none",
        ecolor=color,
        elinewidth=3.0,
        capsize=7,
        capthick=3.0,
        zorder=2,
    )
    ax.scatter(
        x_positions,
        y_values,
        s=65,
        color="white",
        edgecolors=color,
        linewidths=2.0,
        zorder=3,
        label=label,
    )


def make_plot(points: list[LimitSummaryPoint], label: str, output_path: Path) -> None:
    x_positions = list(range(len(points)))
    x_labels = [point.tick_label for point in points]
    y_values = [point.expected * PB_TO_FB for point in points]
    ymax = max(point.expected_p2sigma * PB_TO_FB for point in points) * 1.15

    fig, ax = plt.subplots(figsize=(13.5, 7.2))
    draw_expected_errorbars(
        ax,
        x_positions,
        points,
        color="#0f766e",
        light_color="#94a3b8",
        label="Median expected",
    )

    ax.set_xticks(x_positions)
    ax.set_xticklabels(x_labels)
    ax.set_xlim(-0.45, len(points) - 0.55)
    ax.set_ylim(0.0, ymax)
    ax.set_xlabel("Signal Mass Point (mX,mY) GeV", fontsize=12, labelpad=12)
    ax.set_ylabel("Cross section x BR [fb]", fontsize=12, labelpad=12)
    ax.set_title(f"{label} discriminator", fontsize=15, pad=18, fontweight="bold")
    style_axes(ax)
    add_prediction_lines(ax, [point.mass_label for point in points], x_positions)
    annotate_points(ax, x_positions, y_values)
    handles = [
        plt.Line2D([0], [0], marker="o", linestyle="none", markerfacecolor="white",
                   markeredgecolor="#0f766e", markeredgewidth=2.0, markersize=8,
                   label="Median expected"),
        plt.Line2D([0], [0], color="#6b7280", linewidth=3.0, label=r"Expected $\pm 1\sigma$"),
        plt.Line2D([0], [0], color="#6b7280", linewidth=2.0, label=r"Expected $\pm 2\sigma$"),
        plt.Line2D([0], [0], color="#ec4899", linewidth=2.0, linestyle="--", label="scalar X"),
        plt.Line2D([0], [0], color="#eab308", linewidth=2.0, linestyle="--", label="pseudoscalar X"),
    ]
    ax.legend(handles=handles, frameon=False, fontsize=11, loc="upper right")
    apply_label(ax, label)
    fig.subplots_adjust(left=0.10, right=0.98, top=0.88, bottom=0.22)

    for ext in ("png", "pdf"):
        fig.savefig(output_path.with_suffix(f".{ext}"), bbox_inches="tight", dpi=900)
    plt.close(fig)


def make_overlay_plot(
    labelled_points: list[tuple[str, list[LimitSummaryPoint]]],
    output_path: Path,
) -> None:
    common_labels = [point.mass_label for point in labelled_points[0][1]]
    common_positions = list(range(len(common_labels)))
    tick_labels = [point.tick_label for point in labelled_points[0][1]]

    fig, ax = plt.subplots(figsize=(13.5, 7.2))
    if len(labelled_points) > 1:
        offset_step = 0.14
        center = (len(labelled_points) - 1) / 2.0
        x_position_sets = [
            [x + (index - center) * offset_step for x in common_positions]
            for index in range(len(labelled_points))
        ]
    else:
        x_position_sets = [common_positions]

    for index, (label, points) in enumerate(labelled_points):
        points_by_label = {point.mass_label: point for point in points}
        ordered_points = [points_by_label[mass_label] for mass_label in common_labels]
        color, light_color = get_label_colors(label, index)
        draw_expected_errorbars(
            ax,
            x_position_sets[index],
            ordered_points,
            color=color,
            light_color=light_color,
            label=label,
        )

    ymax = max(
        point.expected_p2sigma * PB_TO_FB
        for _, points in labelled_points
        for point in points
    ) * 1.15

    ax.set_xticks(common_positions)
    ax.set_xticklabels(tick_labels)
    ax.set_xlim(-0.45, len(common_labels) - 0.55)
    ax.set_ylim(0.0, ymax)
    ax.set_xlabel("Signal Mass Point (mX,mY) GeV", fontsize=12, labelpad=12)
    ax.set_ylabel("Cross section x BR [fb]", fontsize=12, labelpad=12)
    style_axes(ax)
    add_prediction_lines(ax, common_labels, common_positions)
    apply_label(ax, "", right_x=0.80, right_ha="center")
    label_aliases = {
        "non-pnn": "non-parametrized",
        "pnn": "PNN",
    }
    model_handles = []
    for index, (label, _) in enumerate(labelled_points):
        color, _ = get_label_colors(label, index)
        model_handles.append(
            plt.Line2D(
                [0], [0],
                marker="o",
                linestyle="none",
                markerfacecolor="white",
                markeredgecolor=color,
                markeredgewidth=2.0,
                markersize=8,
                label=label_aliases.get(label.lower(), label),
            )
        )

    limits_handles = model_handles + [
        plt.Line2D([0], [0], color="#6b7280", linewidth=3.0, label="Expected +/-1sigma"),
        plt.Line2D([0], [0], color="#6b7280", linewidth=2.0, label="Expected +/-2sigma"),
    ]
    limits_legend = ax.legend(
        handles=limits_handles,
        title="Median expected 95% CL limits",
        frameon=False,
        fontsize=10.5,
        title_fontsize=11,
        loc="upper center",
        bbox_to_anchor=(0.80, 1.0),
    )
    ax.add_artist(limits_legend)

    prediction_handles = [
        plt.Line2D([0], [0], color="#ec4899", linewidth=2.0, linestyle="--", label="scalar X"),
        plt.Line2D([0], [0], color="#eab308", linewidth=2.0, linestyle="--", label="pseudoscalar X"),
    ]
    ax.legend(
        handles=prediction_handles,
        title="NMSSM prediction\nLHCHWG-2026-002",
        frameon=False,
        fontsize=10.5,
        title_fontsize=11,
        loc="upper center",
        bbox_to_anchor=(0.80, 0.67),
    )
    fig.subplots_adjust(left=0.10, right=0.98, top=0.88, bottom=0.22)

    for ext in ("png", "pdf"):
        fig.savefig(output_path.with_suffix(f".{ext}"), bbox_inches="tight", dpi=900)
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
        default=Path("limit_plots/output/median_expected_1d_errorbars"),
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
        make_plot(points, label, output_dir / f"{slugify(label)}_median_expected_1d_errorbars")
        labelled_points.append((label, points))

    if len(labelled_points) > 1:
        baseline_labels = [point.mass_label for point in labelled_points[0][1]]
        if all([point.mass_label for point in points] == baseline_labels for _, points in labelled_points[1:]):
            make_overlay_plot(labelled_points, output_dir / "comparison_median_expected_1d_errorbars")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
