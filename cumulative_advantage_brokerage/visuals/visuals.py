from typing import Tuple, List, Optional

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle, ConnectionPatch

from ..constants import N_STAGES

def draw_zooming_box(ax_source: plt.Axes, xy_source: Tuple[float, float], size_box: Tuple[float, float] = (.25, .25)) -> Rectangle:
    box_width, box_height = size_box
    box_left = Rectangle(
        xy_source,
        box_width, box_height, linewidth=1, edgecolor="gray", facecolor="none", zorder=0)
    ax_source.add_patch(box_left)
    return box_left

def draw_zooming_edge(fig: plt.Figure, xy_source: Tuple[float, float], xy_target: Tuple[float, float], ax_source: plt.Axes, ax_target: plt.Axes) -> ConnectionPatch:
    top_right_conn = ConnectionPatch(
    xyA=xy_source,
    xyB=xy_target,
    coordsA='data', coordsB='axes fraction',
    axesA=ax_source, axesB=ax_target, color='gray', lw=1,
    zorder=1)
    fig.add_artist(top_right_conn)
    return top_right_conn

def plot_cdfs(
        ax: plt.Axes,
        l_cdf: List[np.ndarray],
        l_stages: List[int],
        color_map,
        l_labels: Optional[List[str]] = None,
        print_ylabel: bool = True,
        ccdf: bool = False):
    if l_labels is None:
        l_labels = [None for _ in range(l_stages)]
    for cdf, stage, label in zip(l_cdf, l_stages, l_labels):
        _cdf_sorted = np.sort(cdf)
        _cdf_vals = (np.arange(len(_cdf_sorted)) / len(_cdf_sorted))
        if ccdf:
            _cdf_vals = 1 - _cdf_vals
        ax.step(
            _cdf_sorted,
            _cdf_vals,
            label=label,
            linewidth=2,
            color=color_map((stage + 1) / N_STAGES)
        )

        ax.spines[["right", "top"]].set_visible(False)
        ax.set_xscale("log")
        if print_ylabel:
            ax.set_ylabel("CDF" if not ccdf else "CCDF")
        # if x_label is not None:
        #     ax.set_xlabel(x_label)
        # _=ax.legend(frameon=False, handlelength=1, loc="lower right")
