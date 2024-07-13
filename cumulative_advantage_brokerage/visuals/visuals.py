from typing import Tuple

import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle, ConnectionPatch

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
