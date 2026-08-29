#!/usr/bin/env python3

from __future__ import annotations

import argparse
import pickle
import re
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


def fold_number(path: Path) -> int:
    """Extract the fold number from a path containing 'fNofM'."""
    match = re.search(r"f(\d+)of(\d+)", str(path))
    return int(match.group(1)) if match else 999


def load_histories(model_root: Path) -> list[tuple[Path, dict]]:
    """Find and load all fold-specific Keras history files."""
    history_files = sorted(
        model_root.rglob("model_history.pkl"),
        key=fold_number,
    )

    if not history_files:
        raise FileNotFoundError(
            f"No model_history.pkl files found below:\n{model_root}\n\n"
            "Point --model-root to the MLTraining v10 directory containing "
            "the mlmodel_f0of4, ..., mlmodel_f3of4 directories."
        )

    histories = []

    for history_file in history_files:
        with history_file.open("rb") as f:
            history = pickle.load(f)

        if "loss" not in history:
            raise KeyError(
                f"'loss' is missing from {history_file}. "
                f"Available quantities: {list(history)}"
            )

        histories.append((history_file, history))

    return histories


def stack_common_epochs(
    histories: list[tuple[Path, dict]],
    key: str,
) -> np.ndarray | None:
    """
    Stack one history quantity after truncating all folds to the shortest
    available training length.
    """
    available = [
        np.asarray(history[key], dtype=float)
        for _, history in histories
        if key in history
    ]

    if not available:
        return None

    if len(available) != len(histories):
        missing = [
            str(path)
            for path, history in histories
            if key not in history
        ]
        raise KeyError(
            f"Quantity '{key}' is missing for some folds:\n"
            + "\n".join(missing)
        )

    common_epochs = min(len(values) for values in available)

    if common_epochs == 0:
        raise ValueError(f"Empty '{key}' history encountered.")

    return np.stack(
        [values[:common_epochs] for values in available],
        axis=0,
    )


def plot_mean_loss(
    model_root: Path,
    output: Path,
    show_individual: bool = False,
    log_y: bool = False,
) -> None:
    histories = load_histories(model_root)

    train_losses = stack_common_epochs(histories, "loss")
    validation_losses = stack_common_epochs(histories, "val_loss")

    if train_losses is None:
        raise RuntimeError("No training-loss histories found.")

    n_folds, n_epochs = train_losses.shape
    epochs = np.arange(1, n_epochs + 1)

    train_mean = np.mean(train_losses, axis=0)
    train_std = np.std(train_losses, axis=0, ddof=1) if n_folds > 1 else np.zeros(n_epochs)

    fig, ax = plt.subplots(figsize=(8.0, 6.0))

    if show_individual:
        for fold_index, fold_loss in enumerate(train_losses):
            ax.plot(
                epochs,
                fold_loss,
                linewidth=0.8,
                alpha=0.25,
                label=f"Training fold {fold_index}" if fold_index == 0 else None,
            )

    train_line = ax.plot(
        epochs,
        train_mean,
        linewidth=2.0,
        label="Mean training loss",
    )[0]

    ax.fill_between(
        epochs,
        train_mean - train_std,
        train_mean + train_std,
        alpha=0.20,
        color=train_line.get_color(),
        label=r"Training $\pm 1\sigma$ across folds",
    )

    if validation_losses is not None:
        # Use the common epoch range shared by both training and validation.
        final_epochs = min(n_epochs, validation_losses.shape[1])

        epochs = epochs[:final_epochs]
        train_mean = train_mean[:final_epochs]
        train_std = train_std[:final_epochs]
        validation_losses = validation_losses[:, :final_epochs]

        validation_mean = np.mean(validation_losses, axis=0)
        validation_std = (
            np.std(validation_losses, axis=0, ddof=1)
            if validation_losses.shape[0] > 1
            else np.zeros(final_epochs)
        )

        if show_individual:
            for fold_index, fold_loss in enumerate(validation_losses):
                ax.plot(
                    epochs,
                    fold_loss,
                    linewidth=0.8,
                    alpha=0.25,
                    linestyle="--",
                    label=f"Validation fold {fold_index}" if fold_index == 0 else None,
                )

        validation_line = ax.plot(
            epochs,
            validation_mean,
            linewidth=2.0,
            linestyle="--",
            label="Mean validation loss",
        )[0]

        ax.fill_between(
            epochs,
            validation_mean - validation_std,
            validation_mean + validation_std,
            alpha=0.20,
            color=validation_line.get_color(),
            label=r"Validation $\pm 1\sigma$ across folds",
        )

        best_epoch = int(np.argmin(validation_mean)) + 1
        best_loss = validation_mean[best_epoch - 1]

        ax.axvline(
            best_epoch,
            linewidth=1.2,
            linestyle=":",
            label=f"Minimum mean validation loss: epoch {best_epoch}",
        )

        print(
            f"Minimum mean validation loss: {best_loss:.6g} "
            f"at epoch {best_epoch}"
        )

    if log_y:
        ax.set_yscale("log")

    ax.set_xlabel("Epoch")
    ax.set_ylabel("Binary cross-entropy loss")
    ax.set_title(f"Mean training history across {n_folds} folds")
    ax.grid(True, alpha=0.25)
    ax.legend(frameon=False)

    fig.tight_layout()

    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=300, bbox_inches="tight")
    plt.close(fig)

    print(f"Loaded {n_folds} fold histories:")
    for path, history in histories:
        print(
            f"  {path} "
            f"({len(history['loss'])} epochs)"
        )

    print(f"Common number of plotted epochs: {n_epochs}")
    print(f"Saved mean loss plot to: {output}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot mean training and validation loss across ML folds."
    )

    parser.add_argument(
        "--model-root",
        type=Path,
        required=True,
        help="MLTraining version directory containing the fold model directories.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Output image path, for example mean_loss.pdf.",
    )
    parser.add_argument(
        "--show-individual",
        action="store_true",
        help="Also draw the individual fold curves faintly.",
    )
    parser.add_argument(
        "--log-y",
        action="store_true",
        help="Use a logarithmic loss axis.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    plot_mean_loss(
        model_root=args.model_root.expanduser().resolve(),
        output=args.output.expanduser().resolve(),
        show_individual=args.show_individual,
        log_y=args.log_y,
    )
