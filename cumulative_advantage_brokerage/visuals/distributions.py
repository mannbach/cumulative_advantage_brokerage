from typing import List, Tuple, Union, Type

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

from .visuals import draw_zooming_box, draw_zooming_edge, plot_cdfs
from ..stats import StatisticalTest, GrouperDummy, Grouper, MannWhitneyPermutTest
from ..constants import OFFSET_MARKERS, N_STAGES, TPL_CM_IMPACT, CM_CITATION, CM_PRODUCTIVITY, WIDTH_FIG_PAPER, HEIGHT_FIG_PAPER, TPL_STR_IMPACT, TPL_CM_IMPACT, CM_CAREER_LENGTH



def setup_ccdf_plot(col_stages: bool, xlabel: str)\
    -> Tuple[plt.Figure, plt.Axes, plt.legend]:
    f,a_ax = plt.subplots(2, (N_STAGES-1) if col_stages else N_STAGES, figsize=(WIDTH_FIG_PAPER, (2/3)*HEIGHT_FIG_PAPER), sharex="col", sharey=True)

    for metric, a_ax_metric in zip(TPL_STR_IMPACT, a_ax):
        for col, ax in enumerate(a_ax_metric):
            ax.set_xscale("log")
            ax.set_yscale("log")
            ax.spines[["top", "right"]].set_visible(False)

    for col, ax in enumerate(a_ax[0]):
        ax.text(.8, .8,
                f"$s_{col}$" if col_stages else f"$Q_{col}$",
                fontsize=10, transform=ax.transAxes)
    for ax in a_ax[-1]:
        ax.set_xlabel(xlabel)
    for ax in a_ax[:,0]:
        ax.set_ylabel("CCDF")
        # Set a minimum of 2 ticks
        ax.yaxis.set_major_locator(ticker.LogLocator(numticks=5))

    l_legends = []
    tpl_pos_y = (.97, .5)
    if col_stages:
        for metric, cm, pos_y in zip(TPL_STR_IMPACT, TPL_CM_IMPACT, tpl_pos_y):
            legend = f.legend(
                [plt.Line2D([], [],
                    color=cm((stage_max+1) / N_STAGES),
                    label=stage_max,
                    marker="o",
                    linestyle="None")\
                        for stage_max in range(N_STAGES)],
                [f"$Q_{m}$" for m in range(N_STAGES)],
                frameon=False,
                ncol=N_STAGES,
                columnspacing=.1,
                handletextpad=.05,
                loc="upper center",
                bbox_to_anchor=(0.5, pos_y))
            # legend.set_title(metric)
            f.text(.5, pos_y-.02, metric, transform=f.transFigure)
            l_legends.append(legend)

        f.subplots_adjust(hspace=.5, wspace=.5)
    else:
        for stage in range(N_STAGES - 1):
            legend = f.legend(
                    [plt.Line2D([], [],
                        color=CM_CAREER_LENGTH((stage+1) / (N_STAGES - 1)),
                        label=stage,
                        marker="o",
                        linestyle="None")\
                            for stage in range(N_STAGES - 1)],
                    [f"$s_{i}$" for i in range(N_STAGES - 1)],
                    frameon=False,
                    ncol=N_STAGES - 1,
                    columnspacing=.1,
                    handletextpad=.05,
                    loc="upper center",
                    bbox_to_anchor=(0.5, tpl_pos_y[0] + .01))
            # legend.set_title(metric)
            l_legends.append(legend)
        for metric, pos_y in zip(TPL_STR_IMPACT, tpl_pos_y):
            f.text(.5, pos_y - .02, metric, transform=f.transFigure)
        f.subplots_adjust(hspace=.4, wspace=.5)

    return f,a_ax,l_legends