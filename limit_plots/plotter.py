#!/usr/bin/env python3
"""Plotting helpers for Combine limit summaries."""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Sequence

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import LogNorm
from matplotlib.ticker import AutoMinorLocator, FuncFormatter, LogLocator, MultipleLocator

from .limits import LimitCollection

try:  # pragma: no cover - optional dependency
    import mplhep

    mplhep.style.use(mplhep.style.CMS)
    _HAVE_MPLHEP = True
except ImportError:  # pragma: no cover - graceful fallback
    plt.style.use("seaborn-v0_8-darkgrid")
    _HAVE_MPLHEP = False


COLORS = {
    "expected": "#004c99",
    "observed": "#bb133e",
    "band_1sigma": "#7fbf7b",
    "band_2sigma": "#fdd866",
}


class LimitPlotter:
    """Create polished plots for limit scans."""

    def __init__(self, output_dir: Path, label: str = "", sqrt_s: str = "13.6") -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.label = label
        self.sqrt_s = sqrt_s

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _sqrt_s_label_values(self) -> tuple[float | str | None, str]:
        sqrt_s_str = str(self.sqrt_s).strip()
        if not sqrt_s_str:
            return None, ""

        if sqrt_s_str.lower().endswith("tev"):
            numeric_part = sqrt_s_str[:-3].strip()
        else:
            numeric_part = sqrt_s_str

        try:
            sqrt_s_numeric = float(numeric_part)
        except ValueError:
            return sqrt_s_str, sqrt_s_str

        display = f"{sqrt_s_numeric:g} TeV"
        return sqrt_s_numeric, display

    def _apply_cms_label(self, ax: plt.Axes) -> None:
        com_value, display_sqrt_s = self._sqrt_s_label_values()

        if _HAVE_MPLHEP:
            mplhep.cms.label(
                ax=ax,
                llabel="Private Work",
                com=com_value if display_sqrt_s else None,
                data=False,
                label=self.label or None,
            )
        else:
            title_left = "CMS"
            if self.label:
                title_left += f" • {self.label}"
            ax.set_title(title_left, loc="left", fontsize=13, fontweight="bold")
            if display_sqrt_s:
                ax.set_title(rf"$\sqrt{{s}} = {display_sqrt_s}$", loc="right", fontsize=11)

    def _prepare_axes(self, ax: plt.Axes, *, logy: bool = True) -> None:
        if logy:
            ax.set_yscale("log")
            ax.yaxis.set_major_locator(LogLocator(base=10))
        else:
            ax.yaxis.set_major_locator(MultipleLocator(50))
        ax.xaxis.set_minor_locator(AutoMinorLocator())
        if not logy:
            ax.yaxis.set_minor_locator(AutoMinorLocator())
        ax.tick_params(axis="both", which="major", direction="in", length=8, labelsize=11)
        ax.tick_params(axis="both", which="minor", direction="in", length=4)
        ax.grid(True, which="both", linestyle="--", linewidth=0.6, alpha=0.4)
        self._apply_cms_label(ax)

    def _create_figure(
        self,
        x_label: str,
        y_label: str,
        *,
        logy: bool = True,
        figsize: Sequence[float] = (8.0, 6.0),
    ) -> tuple[plt.Figure, plt.Axes]:
        fig, ax = plt.subplots(figsize=figsize)
        ax.set_xlabel(x_label, ha="right", x=1.0)
        ax.set_ylabel(y_label, ha="right", y=1.0)
        self._prepare_axes(ax, logy=logy)
        fig.tight_layout()
        return fig, ax

    @staticmethod
    def _set_axis_span(ax: plt.Axes, values: Sequence[float], *, axis: str = "x") -> None:
        if not values:
            return
        vmin = min(values)
        vmax = max(values)
        if vmin == vmax:
            pad = max(1.0, 0.05 * abs(vmin))
            limits = (vmin - pad, vmax + pad)
        else:
            pad = 0.05 * (vmax - vmin)
            limits = (vmin - pad, vmax + pad)
        if axis == "x":
            ax.set_xlim(*limits)
        else:
            ax.set_ylim(*limits)

    def _save(self, fig: plt.Figure, basename: str) -> List[Path]:
        outputs: List[Path] = []
        for ext in ("png", "pdf"):
            path = self.output_dir / f"{basename}.{ext}"
            save_kwargs = {"dpi": 300} if ext == "png" else {}
            fig.savefig(path, bbox_inches="tight", **save_kwargs)
            outputs.append(path)
        plt.close(fig)
        return outputs

    # ------------------------------------------------------------------
    # Plotting routines
    # ------------------------------------------------------------------
    def plot_expected(self, collection: LimitCollection, mass_y: int) -> Optional[List[Path]]:
        xs, expected = collection.expected_curve(mass_y)
        if len(xs) < 2:
            return None

        fig, ax = self._create_figure(
            x_label=r"$m_X$ [GeV]",
            y_label=r"$95\%$ CL limit on $\sigma \times \mathrm{BR}$ [pb]",
            logy=True,
        )

        ax.plot(
            xs,
            expected,
            marker="o",
            markersize=5,
            linewidth=2.0,
            color=COLORS["expected"],
            label="Median expected",
        )

        obs_x, obs_y = collection.observed_curve(mass_y)
        if len(obs_x) >= 2:
            ax.plot(
                obs_x,
                obs_y,
                marker="s",
                markersize=5,
                linewidth=1.8,
                linestyle="--",
                color=COLORS["observed"],
                label="Observed",
            )

        self._set_axis_span(ax, xs, axis="x")

        ax.legend(
            title=rf"$m_Y = {mass_y}\,\mathrm{{GeV}}$",
            loc="upper right",
            frameon=False,
            fontsize=11,
            title_fontsize=11,
        )

        return self._save(fig, f"limits_expected_mY{mass_y}")

    def plot_brazil(self, collection: LimitCollection, mass_y: int) -> Optional[List[Path]]:
        xs, expected = collection.expected_curve(mass_y)
        if len(xs) < 2:
            return None

        xs_1, lower_1, upper_1 = collection.expected_band(mass_y, (16.0, 84.0))
        xs_2, lower_2, upper_2 = collection.expected_band(mass_y, (2.5, 97.5))
        if not xs_1 or not xs_2:
            return None

        fig, ax = self._create_figure(
            x_label=r"$m_X$ [GeV]",
            y_label=r"$95\%$ CL limit on $\sigma \times \mathrm{BR}$ [pb]",
            logy=True,
        )

        ax.fill_between(
            xs_2,
            lower_2,
            upper_2,
            color=COLORS["band_2sigma"],
            alpha=0.8,
            label=r"Expected $\pm 2\sigma$",
        )
        ax.fill_between(
            xs_1,
            lower_1,
            upper_1,
            color=COLORS["band_1sigma"],
            alpha=0.9,
            label=r"Expected $\pm 1\sigma$",
        )
        ax.plot(
            xs,
            expected,
            marker="o",
            markersize=5,
            linewidth=2.0,
            color="black",
            linestyle="--",
            label="Median expected",
        )

        obs_x, obs_y = collection.observed_curve(mass_y)
        if len(obs_x) >= 2:
            ax.plot(
                obs_x,
                obs_y,
                marker="s",
                markersize=5,
                linewidth=1.8,
                linestyle="-",
                color=COLORS["observed"],
                label="Observed",
            )

        self._set_axis_span(ax, xs, axis="x")

        ax.legend(
            title=rf"$m_Y = {mass_y}\,\mathrm{{GeV}}$",
            loc="upper right",
            frameon=False,
            fontsize=11,
            title_fontsize=11,
        )

        return self._save(fig, f"limits_brazil_mY{mass_y}")

    def plot_heatmap(self, collection: LimitCollection) -> Optional[List[Path]]:
        xs, ys, values = collection.heatmap_data()
        if len(xs) < 3:
            return None

        values_array = np.asarray(values)
        positive = values_array[values_array > 0]
        if len(positive) == 0:
            return None

        vmin = positive.min()
        vmax = positive.max()
        norm = LogNorm(vmin=vmin, vmax=vmax)

        fig, ax = self._create_figure(
            x_label=r"$m_X$ [GeV]",
            y_label=r"$m_Y$ [GeV]",
            logy=False,
            figsize=(8.5, 6.5),
        )

        contour = ax.tricontourf(
            xs,
            ys,
            values_array,
            levels=np.geomspace(vmin, vmax, num=30),
            cmap="viridis",
            norm=norm,
        )
        ax.tricontour(
            xs,
            ys,
            values_array,
            levels=np.geomspace(vmin, vmax, num=6),
            colors="white",
            linewidths=0.6,
            linestyles="solid",
        )
        scatter = ax.scatter(
            xs,
            ys,
            c=values_array,
            cmap="viridis",
            norm=norm,
            edgecolors="white",
            linewidths=0.6,
            s=50,
        )

        for mx, my, val in zip(xs, ys, values_array):
            text_color = "white" if norm(val) > 0.6 else "#1b1b1b"
            ax.text(
                mx,
                my,
                f"{val:.2g}",
                color=text_color,
                fontsize=8,
                fontweight="semibold",
                ha="center",
                va="center",
            )

        cbar = fig.colorbar(
            scatter,
            ax=ax,
            pad=0.015,
            aspect=30,
            label=r"$95\%$ CL limit on $\sigma \times \mathrm{BR}$ [pb]",
        )
        def _pb_tick_formatter(value: float, _: int) -> str:
            if value <= 0:
                return r"$0$"
            exponent = int(np.floor(np.log10(value)))
            if exponent >= 0:
                return rf"${value:.3g}\,\mathrm{{pb}}$"
            mantissa = value / (10.0 ** exponent)
            if mantissa >= 10.0:
                mantissa /= 10.0
                exponent += 1
            mantissa_str = f"{mantissa:.2g}"
            return rf"${mantissa_str}\times 10^{{{exponent}}}\,\mathrm{{pb}}$"

        cbar.ax.yaxis.set_major_formatter(FuncFormatter(_pb_tick_formatter))
        cbar.update_ticks()
        cbar.ax.tick_params(labelsize=8)

        self._set_axis_span(ax, xs, axis="x")
        self._set_axis_span(ax, ys, axis="y")

        return self._save(fig, "limits_heatmap")
