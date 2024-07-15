import os
from typing import List, Union, Optional, Dict
import warnings
from itertools import product
from collections import defaultdict
from datetime import datetime

import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm, CenteredNorm, TwoSlopeNorm
from matplotlib.ticker import ScalarFormatter
from mpl_toolkits.axes_grid1.inset_locator import inset_axes
import numpy as np
import scipy as sc
import pandas as pd

from ..dbm import GENDER_FEMALE, GENDER_MALE, GENDER_UNKNOWN
from ..constants import\
    L_MOTIF_GEN_SORTED_AGG, D_MOTIF_GEN_SORTED_AGG_MISSING,\
    DURATION_BUFFER_AUTHOR_ACTIVE, DATE_OBSERVATION_END,\
    DATE_OBSERVATION_START, M_GENDER_LABEL, CM_GENDER_TRIPLET, CM_GENDER, N_STAGES, SIZE_FIG_PAPER

MIN_YEAR = DATE_OBSERVATION_START.year
MAX_YEAR = (DATE_OBSERVATION_END - DURATION_BUFFER_AUTHOR_ACTIVE).year
XTICKS_YEARS = [1920, 1960, 2000]

def plot_gen_brok_evolution(
        gs_gabc_y_cnt: pd.Grouper,
        ax: Optional[plt.Axes] = None,
        aggregate_mixed: bool = False,
        plot_init: bool = True,
        plot_init_cnt: bool = True,
        normalize: bool = False,
        s_auth_active_gender = None,
        size_marker: float = 50.):
    fig = None
    if ax is None:
        fig, ax = plt.subplots(figsize=(SIZE_FIG_PAPER[0], .5*SIZE_FIG_PAPER[1]))
    for t_gender in L_MOTIF_GEN_SORTED_AGG:
        gs_y_cnt = gs_gabc_y_cnt.loc[t_gender].sort_index()
        if aggregate_mixed:
            for t_gender_missing in D_MOTIF_GEN_SORTED_AGG_MISSING[t_gender]:
                gs_y_cnt = gs_y_cnt.add(gs_gabc_y_cnt.loc[t_gender_missing], fill_value=0)
                gs_y_cnt = gs_y_cnt.astype(int).sort_index()

        gs_y_cnt = gs_y_cnt[gs_y_cnt.index < MAX_YEAR]

        t_min = gs_y_cnt.index.min()
        cnt_min = gs_y_cnt.loc[t_min] if plot_init_cnt else .5

        if normalize:
            _cnt_gen = defaultdict(int)
            for g in t_gender:
                _cnt_gen[g] += 1

            _prod = 1.
            for g in (GENDER_FEMALE, GENDER_MALE):
                _s_n = s_auth_active_gender[g.gender]
                _prod *= _s_n.apply(
                    lambda n:\
                        sc.special.comb(n, _cnt_gen[g.gender],
                                        exact=True))

            gs_y_cnt /= _prod

        gs_y_cnt = gs_y_cnt.reindex(
            pd.RangeIndex(
                start=MIN_YEAR,
                stop=MAX_YEAR + 1))

        ax.plot(
            gs_y_cnt,
            color=CM_GENDER_TRIPLET[t_gender],
            label="".join(
                map(lambda g: M_GENDER_LABEL[g][0], t_gender)),
            linewidth=1,
            marker="o",
            markersize=2,
            linestyle="none"
        )

        if plot_init:
            ax.scatter(
                x=[t_min], y=[cnt_min],
                color=CM_GENDER_TRIPLET[t_gender],
                marker="*", s=size_marker)
    ax.spines[["top", "right"]].set_visible(False)
    ax.set_yscale("log")
    ax.set_xlabel("year", labelpad=0.8)
    ax.set_ylabel("brokerage count", labelpad=2.)
    return fig, ax

def plot_new_authors_evolution(
        ax: plt.Axes,
        s_auth_active_gen: Dict[str, pd.Series],
        cumulative: bool=False,
        ylabel: Optional[str] = None,
        yscale: str = "log") -> List[plt.Line2D]:
    l_mar = []
    for gen in (GENDER_MALE, GENDER_FEMALE):
        s_birth_cnt = s_auth_active_gen[gen.gender].sort_index()

        s_birth_cnt = s_birth_cnt.loc[s_birth_cnt.index < MAX_YEAR]

        s_birth_cnt_cum = s_birth_cnt.cumsum()\
            if cumulative else s_birth_cnt

        mar = ax.plot(
            s_birth_cnt_cum,
            color=CM_GENDER[gen.gender],
            label=M_GENDER_LABEL[gen.gender],
            linewidth=2)
        l_mar.append(mar)
    ax.spines[["right", "top"]].set_visible(False)
    ax.set_yscale(yscale)
    formatter = ScalarFormatter(useMathText=True)
    formatter.set_powerlimits((-2, 2))
    ax.yaxis.set_major_formatter(formatter)
    if ylabel is None:
        ylabel = ("cum. " if cumulative else "") + "active authors"
    ax.set_ylabel(ylabel, labelpad=2)

    return l_mar

def plot_heatmap_gender_grid(
        l_ax: List[plt.Axes],
        a_histograms: np.ndarray,
        comparison: bool = False,
        print_titles: Union[bool, List[str]] = True,
        show_y_right: bool = False,
        orient_h: bool = True,
        show_single_label: bool = True) -> List[plt.colorbar]:
    l_cb = []
    vmin, vmax = np.min(np.abs(a_histograms)), np.max(np.abs(a_histograms))
    args_fig = {"cmap": "inferno_r", "vmin":vmin, "vmax": vmax,}
    if comparison:
        # Compute the max distance based on ratio
        # Note that vmax = max(vmax, 1/vmin) and vmin = 1/vmax
        vmax = np.max([vmax, 1/vmin])
        vmin = 1/vmax
        args_fig = {
            "cmap":"PRGn",
            "norm":TwoSlopeNorm(
                vmin=vmin, vcenter=1., vmax=vmax)}
    for ax, a_hist in zip(l_ax, a_histograms):
        cb = ax.imshow(
            a_hist,
            origin="lower",
            **args_fig)
        l_cb.append(cb)

    if isinstance(print_titles, list):
        for ax, title in zip(l_ax, print_titles):
            ax.set_title(title, fontsize=10)

    for i, ax in enumerate(l_ax):
        if orient_h:
            ax.set_xticks(
                np.arange(N_STAGES-1),
                labels=[f"$s_{i}$" for i in range(N_STAGES-1)])
            ax.set_xlabel("stage of $c$", labelpad=1.25)
            if i == 0:
                ax.set_yticks(
                    np.arange(N_STAGES-1),
                    labels=[f"$s_{i}$" for i in range(N_STAGES-1)])
                if show_single_label:
                    ax.set_ylabel("stage of $a$", labelpad=1.25)
                else:
                    ax.tick_params(axis='y',
                                which='major',
                                left=True,
                                labelleft=False)
            else:
                ax.set_yticks(
                    np.arange(N_STAGES-1),
                    labels=[])

            # elif show_single_label:
        else:
            ax.set_yticks(
                np.arange(N_STAGES-1),
                labels=[f"$s_{i}$" for i in range(N_STAGES-1)])
            ax.set_ylabel("stage of $a$", labelpad=2.)
            if i < (len(l_ax) - 1):
                ax.tick_params(axis='y',
                            which='both',
                            bottom=True,
                            labelbottom=False)
            elif show_single_label:
                ax.set_xlabel("stage of $c$")
            ax.set_xticks(
                np.arange(N_STAGES-1),
                labels=[f"$s_{i}$" for i in range(N_STAGES-1)]\
                    if show_single_label else [])

    if show_y_right:
        l_ax[0].yaxis.tick_right()
        l_ax[0].yaxis.set_label_position("right")
    return l_cb, vmin, vmax

def plot_barplots(
        l_ax: List[plt.Axes],
        s_gacb_cnt: pd.Series):
    for ax, (g_a, g_c) in zip(l_ax, product((GENDER_FEMALE, GENDER_MALE), repeat=2)):
        s_gb_cnt = s_gacb_cnt.loc[(g_a.gender, g_c.gender)]
        f_w_b = s_gb_cnt[GENDER_FEMALE.gender] /\
            (s_gb_cnt[GENDER_FEMALE.gender] + s_gb_cnt[GENDER_MALE.gender])
        f_m_b = 1 - f_w_b
        ax.bar(
            [0,1],
            [f_w_b, f_m_b],
            width=.35,
            color=(CM_GENDER[GENDER_FEMALE.gender], CM_GENDER[GENDER_MALE.gender])
        )
        ax.set_xticks([])
        ax.set_yscale("log")
        ax.spines[["right", "top"]].set_visible(False)
    for ax in l_ax[1:]:
        ax.tick_params(axis='y', which='both', left=True, labelleft=False)
    for ax in l_ax:
        ax.set_xticks([0,1], labels=["w","m"])
        ax.tick_params(axis='x', which='major', pad=2)
    l_ax[0].set_ylabel(r"$\%g_b$", labelpad=2.)

def plot_b_hists(
        a_histograms_b: np.ndarray,
        l_ax: List[plt.Axes],
        print_xticks: bool = True,
        print_ylabel: bool = True):
    for ax, a_hist_gb in zip(l_ax, a_histograms_b):
        for gender_b, a_h, offset in\
                zip((GENDER_FEMALE, GENDER_MALE),
                    a_hist_gb,
                    (-.125, .125)):
            ax.bar(
                x=np.arange(N_STAGES - 1) + offset,
                height=a_h,
                width=.25,
                color=CM_GENDER[gender_b.gender],
                # alpha=.6
            )
        ax.spines[["top", "right"]].set_visible(False)
        ax.set_xlabel(r"stage of $b$", labelpad=1.25)

    if print_ylabel:
        l_ax[0].set_ylabel("$P(b)$", labelpad=1.25)
    for ax in l_ax:
        if print_xticks:
            ax.set_xticks(
                range(N_STAGES - 1),
                labels=[f"$s_{i}$" for i in range(N_STAGES - 1)])
        else:
            ax.set_xticks(range(N_STAGES - 1), [])
    for ax in l_ax[1:]:
        ax.tick_params(
            axis='y',
            which='both',
            left=True,
            labelleft=False)

def plot_gender_seniority(
        gs_gabc_y_cnt: pd.Series,
        s_gacb_cnt: pd.Series,
        a_histograms_b: np.ndarray,
        a_h_cmp_joint: np.ndarray,
        a_histograms: np.ndarray,
        a_hist_joint: np.ndarray
):
    plt.rcParams.update({'font.size': 11})

    l_grid_col = np.asarray([0, 4, 5, 9, 11, 15, 19, 23, 27, 29])
    l_grid_row = np.asarray([0, 2, 4, 7])

    fig = plt.figure(
        figsize=(
            SIZE_FIG_PAPER[0],
            .8115*SIZE_FIG_PAPER[1]),
        constrained_layout=True)

    grid = plt.GridSpec(l_grid_row[-1], l_grid_col[-1],
                        figure=fig,
                        wspace=.00005,
                        hspace=.00005)

    ax_tcm = fig.add_subplot(grid[l_grid_row[0]:l_grid_row[2],
                                l_grid_col[0]:l_grid_col[4]])
    ax_h_exmpl = fig.add_subplot(
        grid[l_grid_row[2]:l_grid_row[3],
            l_grid_col[0]:l_grid_col[1]])
    ax_h_norm = fig.add_subplot(
        grid[l_grid_row[2]:l_grid_row[3],
            l_grid_col[2]:l_grid_col[3]])

    ax_cb_exmpl = fig.add_subplot(
        grid[l_grid_row[2]:l_grid_row[3],
            l_grid_col[3]:l_grid_col[4]])
    box = ax_cb_exmpl.get_position()
    ax_cb_exmpl.set_position([
        box.x0 - .00875,
        box.y0 + .01,
        box.width * 0.15,
        box.height * 0.60])

    ax_bar_ww = fig.add_subplot(grid[l_grid_row[0]:l_grid_row[1],
                                    l_grid_col[4]:l_grid_col[5]])
    ax_bar_wm = fig.add_subplot(grid[l_grid_row[0]:l_grid_row[1],
                                    l_grid_col[5]:l_grid_col[6]],
                                    sharex=ax_bar_ww,
                                    sharey=ax_bar_ww
                                    )
    ax_bar_mw = fig.add_subplot(grid[l_grid_row[0]:l_grid_row[1],
                                    l_grid_col[6]:l_grid_col[7]],
                                    sharex=ax_bar_ww,
                                    sharey=ax_bar_ww
                                    )
    ax_bar_mm = fig.add_subplot(grid[l_grid_row[0]:l_grid_row[1],
                                    l_grid_col[7]:l_grid_col[8]],
                                    sharex=ax_bar_ww,
                                    sharey=ax_bar_ww
                                    )
    l_ax_bar = (ax_bar_ww, ax_bar_wm, ax_bar_mw, ax_bar_mm)

    ax_hist_ww = fig.add_subplot(grid[l_grid_row[2]:l_grid_row[3],
                                    l_grid_col[4]:l_grid_col[5]])
    ax_hist_wm = fig.add_subplot(grid[l_grid_row[2]:l_grid_row[3],
                                    l_grid_col[5]:l_grid_col[6]])
    ax_hist_mw = fig.add_subplot(grid[l_grid_row[2]:l_grid_row[3],
                                    l_grid_col[6]:l_grid_col[7]])
    ax_hist_mm = fig.add_subplot(grid[l_grid_row[2]:l_grid_row[3],
                                    l_grid_col[7]:l_grid_col[8]])
    l_ax_hist = (ax_hist_ww,ax_hist_wm,ax_hist_mw,ax_hist_mm)

    ax_cb_hist = fig.add_subplot(grid[l_grid_row[2]:l_grid_row[3],
                                    l_grid_col[8]:l_grid_col[9]])
    box = ax_cb_hist.get_position()
    ax_cb_hist.set_position([
        box.x0 + .148,
        box.y0 + .01,
        box.width * 0.15,
        box.height * 0.60])

    ax_b_hist_ww = fig.add_subplot(grid[l_grid_row[1]:l_grid_row[2],
                                        l_grid_col[4]:l_grid_col[5]])
    ax_b_hist_wm = fig.add_subplot(grid[l_grid_row[1]:l_grid_row[2],
                                        l_grid_col[5]:l_grid_col[6]],
                                        sharex=ax_b_hist_ww, sharey=ax_b_hist_ww
                                        )
    ax_b_hist_mw = fig.add_subplot(grid[l_grid_row[1]:l_grid_row[2],
                                        l_grid_col[6]:l_grid_col[7]],
                                        sharex=ax_b_hist_ww, sharey=ax_b_hist_ww
                                        )
    ax_b_hist_mm = fig.add_subplot(grid[l_grid_row[1]:l_grid_row[2],
                                        l_grid_col[7]:l_grid_col[8]],
                                        sharex=ax_b_hist_ww, sharey=ax_b_hist_ww
                                        )
    l_ax_hist_b = (ax_b_hist_ww, ax_b_hist_wm, ax_b_hist_mw, ax_b_hist_mm)

    plot_gen_brok_evolution(
        gs_gabc_y_cnt=gs_gabc_y_cnt,
        ax=ax_tcm,
        aggregate_mixed=True,
        plot_init_cnt=False,
        plot_init=True)
    ax_tcm.set_ylim(bottom=.01)
    ax_tcm.set_yticks([10**0, 10**2, 10**4])
    ax_tcm.set_ylabel("brokerage count", labelpad=.8)
    ax_tcm.set_xticks(XTICKS_YEARS)

    plot_barplots(s_gacb_cnt=s_gacb_cnt, l_ax=l_ax_bar)
    l_ax_bar[0].set_ylabel(r"$P(g_b)$", labelpad=.3)
    for ax_bar in l_ax_bar:
        ax_bar.set_xlabel(r"$g_b$", labelpad=0)

    plot_b_hists(
        a_histograms_b=a_histograms_b,
        l_ax=l_ax_hist_b,
        print_xticks=True)
    l_ax_hist_b[0].set_ylabel(r"$P(s_i)$", labelpad=.5)

    l_cb_hist, vmin, vmax = plot_heatmap_gender_grid(
        l_ax=l_ax_hist,
        a_histograms=a_h_cmp_joint,
        comparison=True,
        print_titles=False,
        show_single_label=False,
        orient_h=True)

    l_cb_cmp, _, _ = plot_heatmap_gender_grid(
        l_ax=[ax_h_exmpl, ax_h_norm],
        a_histograms=[a_histograms[0], a_hist_joint],
        print_titles=False,
        orient_h=True,
        show_single_label=True
    )
    ax_h_exmpl.axline((1,1), slope=1., color="black", linestyle="dashed")
    ax_h_norm.axline((1,1), slope=1., color="black", linestyle="dashed")

    fmt = ScalarFormatter(useMathText=True)
    fmt.set_powerlimits((0, 1))
    fig.colorbar(
        l_cb_cmp[-1],
        cax=ax_cb_exmpl,
        format=fmt)

    cb = fig.colorbar(
        l_cb_hist[-1],
        cax=ax_cb_hist)
    cb.set_ticks([vmin, 1., vmax])
    cb.set_ticklabels([f"{v:.1f}" for v in [vmin, 1., vmax]])
    return fig

def plot_author_gender_evolution(
        s_auth_active_gen: pd.Series
):
    fig, ax_mw = plt.subplots(
        figsize=((9/25) * SIZE_FIG_PAPER[0] * 0.8,
                 (3/22) * SIZE_FIG_PAPER[1]))
    plot_new_authors_evolution(
        ax=ax_mw,
        s_auth_active_gen=s_auth_active_gen,
        cumulative=False,
        yscale="linear")
    ax_mw.set_ylabel("active\nauthors", labelpad=.8)
    ax_mw.set_xticks(XTICKS_YEARS)
    ax_mw.tick_params(axis='x', which='major', pad=.8)
    return fig
