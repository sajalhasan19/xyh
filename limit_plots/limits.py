#!/usr/bin/env python3
"""Utilities for parsing Combine limit logs."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple


EXPECTED_LABELS: Dict[float, str] = {
    2.5: "exp_m2sigma",
    16.0: "exp_m1sigma",
    50.0: "exp",
    84.0: "exp_p1sigma",
    97.5: "exp_p2sigma",
}


@dataclass(frozen=True)
class LimitPoint:
    mass_x: int
    mass_y: int
    tag: str
    expected: Dict[str, float]
    observed: Optional[float]

    def scaled(self, factor: float, mode: str = "multiply") -> "LimitPoint":
        if mode not in {"multiply", "divide"}:
            raise ValueError(f"Unsupported scale mode '{mode}'")
        if factor == 1.0:
            return self
        if mode == "divide":
            expected = {key: value / factor for key, value in self.expected.items()}
            observed = None if self.observed is None else self.observed / factor
        else:
            expected = {key: value * factor for key, value in self.expected.items()}
            observed = None if self.observed is None else self.observed * factor
        return LimitPoint(
            mass_x=self.mass_x,
            mass_y=self.mass_y,
            tag=self.tag,
            expected=expected,
            observed=observed,
        )


class LimitCollection:
    """Gather limits from a directory containing Combine log files."""

    def __init__(
        self,
        log_dir: Path,
        tag: str,
        signals: Optional[Sequence[str]] = None,
        scale_factor: float = 1.0,
        scale_mode: str = "multiply",
        scale_lookup: Optional[Callable[[int, int], float]] = None,
    ) -> None:
        self.log_dir = Path(log_dir)
        self.tag = tag
        self.scale_factor = scale_factor if scale_factor and scale_factor > 0 else 1.0
        if scale_mode not in {"multiply", "divide"}:
            raise ValueError(f"Unsupported scale_mode '{scale_mode}'")
        self.scale_mode = scale_mode
        self.scale_lookup = scale_lookup
        self._signal_points = self._parse_signal_list(signals)
        self.points: List[LimitPoint] = self._collect_points()
        if not self.points:
            raise RuntimeError(f"No limit logs found under {self.log_dir}")

    @staticmethod
    def _parse_signal_list(signals: Optional[Sequence[str]]) -> Optional[List[Tuple[int, int]]]:
        if not signals:
            return None
        result: List[Tuple[int, int]] = []
        for item in signals:
            item = item.strip()
            if not item or item.startswith("#"):
                continue
            try:
                mass_x_str, mass_y_str = item.split("_")
                result.append((int(mass_x_str), int(mass_y_str)))
            except Exception as exc:  # noqa: BLE001 - want to raise context
                raise ValueError(f"Cannot parse signal entry '{item}'") from exc
        return sorted(set(result))

    @staticmethod
    def _extract_masses(name: str, tag: str) -> Tuple[int, int]:
        tag_pattern = re.escape(tag)
        match = re.search(rf"(?:^|_)x(\d+)_y(\d+)_{tag_pattern}\.log$", name)
        if match:
            return int(match.group(1)), int(match.group(2))
        match = re.search(rf"(?:^|_)(\d+)_(\d+)_{tag_pattern}\.log$", name)
        if match:
            return int(match.group(1)), int(match.group(2))
        raise ValueError(f"Cannot extract masses from {name}")

    def _candidate_logs(self) -> Iterable[Tuple[int, int, Path]]:
        def resolve_path(mass_x: int, mass_y: int) -> Path:
            candidates = sorted(self.log_dir.glob(f"AsymptoticLimits_*x{mass_x}_y{mass_y}_{self.tag}.log"))
            if not candidates:
                candidates = sorted(self.log_dir.glob(f"AsymptoticLimits_{mass_x}_{mass_y}_{self.tag}.log"))
            if not candidates:
                raise FileNotFoundError(
                    f"Missing log file for ({mass_x}, {mass_y}) in {self.log_dir}; "
                    f"expected e.g. AsymptoticLimits_ml_*_x{mass_x}_y{mass_y}_{self.tag}.log",
                )
            return candidates[0]

        if self._signal_points:
            for mass_x, mass_y in self._signal_points:
                path = resolve_path(mass_x, mass_y)
                yield mass_x, mass_y, path
        else:
            for path in sorted(self.log_dir.glob(f"AsymptoticLimits_*_{self.tag}.log")):
                try:
                    mass_x, mass_y = self._extract_masses(path.name, self.tag)
                except ValueError:
                    continue
                yield mass_x, mass_y, path

    @staticmethod
    def _parse_log(path: Path, tag: str) -> LimitPoint:
        expected: Dict[str, float] = {}
        observed: Optional[float] = None

        expected_pattern = re.compile(r"Expected\s+(\d+\.\d+)%:\s+r\s*<\s*([0-9.eE+-]+)")
        observed_pattern = re.compile(r"Observed Limit:\s+r\s*<\s*([0-9.eE+-]+)")

        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                exp_match = expected_pattern.match(line)
                if exp_match:
                    quantile = float(exp_match.group(1))
                    label = EXPECTED_LABELS.get(quantile)
                    if label:
                        expected[label] = float(exp_match.group(2))
                    continue
                obs_match = observed_pattern.match(line)
                if obs_match:
                    observed = float(obs_match.group(1))

        missing = [label for label in EXPECTED_LABELS.values() if label not in expected]
        if missing:
            raise ValueError(f"{path}: missing expected quantiles: {', '.join(missing)}")

        try:
            mass_x, mass_y = LimitCollection._extract_masses(path.name, tag)
        except ValueError as exc:
            raise ValueError(f"Cannot extract masses from {path}") from exc

        return LimitPoint(
            mass_x=mass_x,
            mass_y=mass_y,
            tag=tag,
            expected=expected,
            observed=observed,
        )

    def _collect_points(self) -> List[LimitPoint]:
        points: List[LimitPoint] = []
        for mass_x, mass_y, path in self._candidate_logs():
            factor = self.scale_factor
            if self.scale_lookup:
                try:
                    lookup_factor = self.scale_lookup(mass_x, mass_y)
                except Exception as exc:  # pragma: no cover - propagated to caller
                    raise RuntimeError(
                        f"Failed to obtain scale factor for signal ({mass_x}, {mass_y})"
                    ) from exc
                if lookup_factor is not None:
                    factor = factor * lookup_factor if factor != 1.0 else lookup_factor
            point = self._parse_log(path, self.tag).scaled(factor, self.scale_mode)
            points.append(point)
        points.sort(key=lambda p: (p.mass_y, p.mass_x))
        return points

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------
    def unique_mass_y(self) -> List[int]:
        return sorted({point.mass_y for point in self.points})

    def expected_curve(self, mass_y: int, quantile: float = 50.0) -> Tuple[List[int], List[float]]:
        label = EXPECTED_LABELS.get(quantile, "exp")
        xs: List[int] = []
        values: List[float] = []
        for point in self.points:
            if point.mass_y != mass_y:
                continue
            if label not in point.expected:
                continue
            xs.append(point.mass_x)
            values.append(point.expected[label])
        order = sorted(range(len(xs)), key=lambda i: xs[i])
        xs = [xs[i] for i in order]
        values = [values[i] for i in order]
        return xs, values

    def observed_curve(self, mass_y: int) -> Tuple[List[int], List[float]]:
        xs: List[int] = []
        values: List[float] = []
        for point in self.points:
            if point.mass_y != mass_y or point.observed is None:
                continue
            xs.append(point.mass_x)
            values.append(point.observed)
        order = sorted(range(len(xs)), key=lambda i: xs[i])
        xs = [xs[i] for i in order]
        values = [values[i] for i in order]
        return xs, values

    def expected_band(
        self,
        mass_y: int,
        quantiles: Tuple[float, float],
    ) -> Tuple[List[int], List[float], List[float]]:
        lower_label = EXPECTED_LABELS.get(min(quantiles))
        upper_label = EXPECTED_LABELS.get(max(quantiles))
        if lower_label is None or upper_label is None:
            raise ValueError(f"Unsupported quantiles {quantiles}")
        xs: List[int] = []
        lower: List[float] = []
        upper: List[float] = []
        for point in self.points:
            if point.mass_y != mass_y:
                continue
            if lower_label not in point.expected or upper_label not in point.expected:
                continue
            xs.append(point.mass_x)
            lower.append(point.expected[lower_label])
            upper.append(point.expected[upper_label])
        order = sorted(range(len(xs)), key=lambda i: xs[i])
        xs = [xs[i] for i in order]
        lower = [lower[i] for i in order]
        upper = [upper[i] for i in order]
        return xs, lower, upper

    def heatmap_data(self) -> Tuple[List[int], List[int], List[float]]:
        xs = [point.mass_x for point in self.points]
        ys = [point.mass_y for point in self.points]
        values = [point.expected["exp"] for point in self.points]
        return xs, ys, values

    def to_rows(self) -> List[List[str]]:
        rows: List[List[str]] = []
        for point in self.points:
            rows.append(
                [
                    str(point.mass_x),
                    str(point.mass_y),
                    point.tag,
                    f"{point.expected['exp_m2sigma']:.6g}",
                    f"{point.expected['exp_m1sigma']:.6g}",
                    f"{point.expected['exp']:.6g}",
                    f"{point.expected['exp_p1sigma']:.6g}",
                    f"{point.expected['exp_p2sigma']:.6g}",
                    "" if point.observed is None else f"{point.observed:.6g}",
                ]
            )
        return rows
