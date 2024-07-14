import matplotlib.pyplot as plt
import numpy as np
from typing import List, Tuple, Dict, Any

from ..constants import\
    N_STAGES, COLOR_BROK, SIZE_FIG_PAPER, STR_CAREER_LENGTH,\
    STR_CITATIONS, STR_PRODUCTIVITY, CM_CAREER_LENGTH,\
    CM_CITATION, CM_PRODUCTIVITY, OFFSET_HET_HIST, XLOGSCALE_HET_HIST

N_BINS_HIST = 15
A_TICK_LABELS_Q_LNG = [.04, .13, .26, .46, .76]
A_TICK_LABELS_Q_CIT = [.1, .25, .33, .47, .7]
A_TICK_LABELS_Q_PRD = [.05, .13, .22, .33, .65]

A_SAMPLE_PAP_T_X = np.asarray([ 0., 5., 12., 20.  ])
A_SAMPLE_BROK_T_X = [ 2.3,  9. , 16. ]
A_SAMPLE_BROK_ROLE_X = "ccba"

A_SAMPLE_PAP_T_Y = np.asarray([ 0. , 11.82, 16, 19, 22.  , 31, 33.  ])
A_SAMPLE_BROK_T_Y = [2.1 , 5.1 , 9., 28]
A_SAMPLE_BROK_ROLE_Y = "cacb"

def plot_quantiles(
        ax: plt.Axes,
        bins_stages: np.ndarray,
        s_vals: np.ndarray,
        color_map,
        offset: float = 0.):
    for i, border in enumerate(bins_stages[1:]):
        ax.axvspan(
            bins_stages[i] + offset, border + offset, color=color_map((i + 1)/N_STAGES), alpha=.3
        )
    # print(bins_stages[-1] + offset, s_vals.max() + offset, bins_stages[-1] + offset)
    ax.axvspan(
        bins_stages[-1] + offset, s_vals.max() + offset, color=color_map(1.), alpha=.3
    )

def plot_quantile_borders(ax: plt.Axes, bins_stages: np.ndarray, offset:float = 0.):
    for border in bins_stages[1:]:
        ax.axvline(x=border + offset, color="black", linestyle="dashed")

def plot_labels(
        ax: plt.Axes,
        l_pos: List[Tuple[float, float]],
        l_labels: List[str] = ["Q"],
        text_kwargs: Dict[str,Any]={}):
    _l_labels = [f"${l_labels[0]}_{i}$" for i, _ in enumerate(l_pos)] if len(l_labels) == 1 else l_labels
    for i, ((x, y), label) in enumerate(zip(l_pos,_l_labels)):
        ax.text(x, y, label, transform=ax.transAxes, **text_kwargs)

def plot_histogram(ax: plt.Axes,
                   s_vals: np.ndarray,
                   metric: str,
                   offset: int = 0,
                   xlog: bool = True) -> np.ndarray:
    bins = np.logspace(
        np.log10(s_vals.min() + offset),
        np.log10(s_vals.max() + offset),
        num=N_BINS_HIST) if xlog else\
            np.linspace(
            s_vals.min(), s_vals.max(), N_BINS_HIST)
    hist,_,_ = ax.hist(s_vals + offset,
            bins=bins,
            density=True,
            facecolor="none",
            edgecolor="gray")

    ax.yaxis.set_major_locator(plt.MaxNLocator(4))
    ax.set_xlabel(metric + (f"$ + {offset}$" if offset > 0 else ""))
    ax.set_yscale("log")
    if xlog:
        ax.set_xscale("log")
    ax.spines[["right", "top"]].set_visible(False)
    return hist, bins

def plot_timelines(ax: plt.Axes):
    for i, a_paper in enumerate((A_SAMPLE_PAP_T_Y, A_SAMPLE_PAP_T_X)):
        ax.plot(a_paper, [i for _ in enumerate(a_paper)], marker="o", color="black")
    for i, (a_brok, a_brok_role) in enumerate(zip(
            (A_SAMPLE_BROK_T_Y, A_SAMPLE_BROK_T_X),
            (A_SAMPLE_BROK_ROLE_Y, A_SAMPLE_BROK_ROLE_X))):
        for x, role in zip(a_brok, a_brok_role):
            ax.annotate(role, xy=(x, i-.15), ha="center", color="white")
            ax.plot(
                a_brok, [i for _ in enumerate(a_brok)],
                marker="o", color=COLOR_BROK,
                linestyle="none", markersize=10)

    ax.set_ylim(-.5, 1.8)
    ax.spines[["right", "top", "left"]].set_visible(False)
    ax.set_axis_off()

def plot_heterogeneity(
        tpl_l_vals: Tuple[List[Any], List[Any], List[Any]],
        tpl_l_bins_stages: Tuple[np.ndarray, np.ndarray, np.ndarray]):
    fig = plt.figure(figsize=(SIZE_FIG_PAPER[0], .6*SIZE_FIG_PAPER[1]),
                     layout="constrained")

    grid = plt.GridSpec(6,2, figure=fig)
    ax_cit = fig.add_subplot(grid[:3,1])
    ax_lng = fig.add_subplot(grid[:3,0])
    ax_prd = fig.add_subplot(grid[3:,1])
    ax_sample = fig.add_subplot(grid[3:,0], sharex=ax_lng)

    for ax, l_vals, bins_stages, metric, color_map, offset, l_pos_q, xlog, l_labels in zip(
        (ax_lng, ax_cit, ax_prd),
        tpl_l_vals,
        tpl_l_bins_stages,
        (STR_CAREER_LENGTH, STR_CITATIONS, STR_PRODUCTIVITY),
        (CM_CAREER_LENGTH, CM_CITATION, CM_PRODUCTIVITY),
        OFFSET_HET_HIST,
        (A_TICK_LABELS_Q_LNG, A_TICK_LABELS_Q_CIT, A_TICK_LABELS_Q_PRD),
        XLOGSCALE_HET_HIST,
        (["$50^\%$","$20^\%$","$15^\%$","$10^\%$", "$5^\%$"], ["Q"], ["Q"])):
        plot_quantiles(
            ax=ax, bins_stages=bins_stages, s_vals=l_vals, offset=offset, color_map=color_map
        )
        plot_histogram(
            ax=ax, s_vals=l_vals,
            offset=offset, metric=metric, xlog=xlog)
        plot_quantile_borders(ax=ax, offset=offset, bins_stages=bins_stages)
        plot_labels(ax=ax, l_pos=[(x, 1.1) for x in l_pos_q], l_labels=l_labels)
        ax.yaxis.set_major_locator(plt.LogLocator(numticks=3))

    plot_quantiles(ax=ax_sample,
                bins_stages=tpl_l_bins_stages[0],
                s_vals=tpl_l_vals[0],
                color_map=CM_CAREER_LENGTH,
                offset=OFFSET_HET_HIST[0])
    plot_quantile_borders(
        ax=ax_sample,
        bins_stages=tpl_l_bins_stages[0],
        offset=OFFSET_HET_HIST[0])
    plot_timelines(ax=ax_sample)
    for _, a_brok, y_b, q_l in zip(
            "yx",
            (A_SAMPLE_BROK_T_Y, A_SAMPLE_BROK_T_X),
            (.36, .84),
            (4, 3)):
        _h = np.histogram(
            np.asarray(a_brok),
            bins=tpl_l_bins_stages[0])[0]
        plot_labels(
            ax=ax_sample,
            l_pos=[(x +.02, y_b) for x in A_TICK_LABELS_Q_LNG][:q_l],
            l_labels=_h[:q_l],
            text_kwargs={"color": COLOR_BROK})

    plot_labels(ax=ax_sample, l_pos=[(x +0.02, -.175) for x in A_TICK_LABELS_Q_LNG], l_labels=["s"])

    ax_lng.xaxis.set_minor_locator(plt.MultipleLocator(2))

    fig.text(0., .95, "A", fontdict={"weight": "bold"}, transform=fig.transFigure)
    fig.text(0., .5, "B", fontdict={"weight": "bold"}, transform=fig.transFigure)
    fig.text(.5, .95, "C", fontdict={"weight": "bold"}, transform=fig.transFigure)
    fig.text(.5, .5, "D", fontdict={"weight": "bold"}, transform=fig.transFigure)

    fig.text(.2, 0.025, "career stages $s_i$", transform=fig.transFigure)
    for x,y in zip((.02, .52, .52), (.92, .92, .41)):
        fig.text(x,y,"PDF", transform=fig.transFigure)
    return fig
