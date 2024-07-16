from typing import List, Tuple, Type

import pandas as pd
import numpy as np
import scipy as sc
import matplotlib.pyplot as plt

from .visuals import draw_zooming_box, draw_zooming_edge, plot_cdfs
from ..stats import StatisticalTest, MannWhitneyPermutTest, SpearmanPermutTest
from ..dbm.models.gender import GENDER_FEMALE, GENDER_MALE
from ..constants import  N_STAGES, TPL_STR_IMPACT, TPL_CM_IMPACT, CM_CAREER_LENGTH, WIDTH_FIG_PAPER, HEIGHT_FIG_PAPER, STR_PRODUCTIVITY

STAGE_MAX_EXAMPLE_CMP = 4
STAGE_EXAMPLE_CMP = 1

STAGE_MAX_EXAMPLE_CORR = 4
STAGE_EXAMPLE_CORR = 2

def plot_spearman_heatmap(
        tpl_rates_example: Tuple[pd.Series, pd.Series],
        ax: plt.Axes):
    x,y = tpl_rates_example

    x,y = map(
        lambda x: sc.stats.rankdata(x, method="ordinal"),
        tpl_rates_example)

    vmin, vmax = min(map(min, (x,y))), max(map(max, (x,y)))


    bins = np.linspace(vmin, vmax, num=10)
    h, _, _ = np.histogram2d(x=x,y=y, bins=bins, density=True)


    cb = ax.imshow(h, origin="lower", cmap=CM_CAREER_LENGTH, aspect="auto")

    ax.set_xticks(ticks=range(10))
    ax.set_yticks(ticks=range(10))

    labels = ["" for x in bins]
    ax.set_xticklabels(labels, rotation=45)
    ax.set_yticklabels(labels)

    ax.xaxis.set_major_locator(plt.MaxNLocator(nbins=3))
    ax.yaxis.set_major_locator(plt.MaxNLocator(nbins=3))

    ax.set_xlabel(f"$rank(R(s_{STAGE_EXAMPLE_CORR}))$")
    ax.set_ylabel(f"$rank(R(s_{STAGE_EXAMPLE_CORR + 1}))$")
    _=ax.text(.05, 1.05, "PDF", fontsize=10, transform=ax.transAxes)

    return cb

def plot_rate_comparison(
        l_ax: List[plt.Axes],
        tpl_d_test,
        stat_test: StatisticalTest,
        plot_xticks: bool = True) -> float:
    y_example = 0.5
    for ax, d_tests, metric, color_map in zip(
            l_ax, tpl_d_test, TPL_STR_IMPACT, TPL_CM_IMPACT):
        gs_sm_tests = d_tests.groupby(["stage_max", "stage_curr", "stage_next"]).first()
        for stage_max in range(N_STAGES):
            a_s = []
            a_res = []
            a_ci = [[], []]
            for stage_curr in range(N_STAGES - 2):
                if (stage_max, stage_curr, stage_curr + 1) not in gs_sm_tests.index:
                    continue
                res = gs_sm_tests.loc[(stage_max, stage_curr, stage_curr + 1)]

                if res["p_value"] < .05:
                    a_s.append(stage_curr)
                    a_res.append(res["test_statistic"])
                    a_ci[0].append(np.abs(np.abs(res["test_statistic"]) - np.abs(res["ci_low"])))
                    a_ci[1].append(np.abs(np.abs(res["ci_high"]) - np.abs(res["test_statistic"])))

                if (stage_max == STAGE_MAX_EXAMPLE_CMP) and (stage_curr == STAGE_EXAMPLE_CMP) and (metric == STR_PRODUCTIVITY):
                    y_example = res["test_statistic"]

            ax.errorbar(
                a_s,
                a_res,
                yerr=a_ci,
                marker="o",
                color=color_map((stage_max+1) / N_STAGES),
            )
            ax.axhline(
                y=stat_test.v_neutral, color="black", linestyle="dashed", zorder=0
            )

    l_ax[0].set_ylabel(stat_test.label_y)
    l_ax[-1].tick_params(labelleft=False)
    for ax, metric in zip(l_ax, TPL_STR_IMPACT):
        ax.spines[["right", "top"]].set_visible(False)
        if not plot_xticks:
            ax.axes.xaxis.set_ticklabels([])

    return y_example

def plot_brokerage_rate_comparison(
    tpl_d_test_cmp: Tuple[pd.DataFrame, pd.DataFrame],
    tpl_d_test_cor: Tuple[pd.DataFrame, pd.DataFrame],
    tpl_rates_cmp: Tuple[pd.Series, pd.Series],
    tpl_rates_cor: Tuple[pd.Series, pd.Series],
    test_cmp: Type[StatisticalTest] = MannWhitneyPermutTest,
    test_cor: Type[StatisticalTest] = SpearmanPermutTest
):
    fig = plt.figure(figsize=(WIDTH_FIG_PAPER, .4 * WIDTH_FIG_PAPER))
    grid = plt.GridSpec(2,8)

    ax_cdf = fig.add_subplot(grid[0,-2:])
    ax_cmp_cit = fig.add_subplot(grid[0,:3])
    ax_cmp_prd = fig.add_subplot(grid[0,3:-2],
                                sharey=ax_cmp_cit)
    ax_heat = fig.add_subplot(grid[1,-2:])
    ax_corr_cit = fig.add_subplot(grid[1,:3])
    ax_corr_prd = fig.add_subplot(
        grid[1,3:-2],
        sharey=ax_corr_cit)
    a_ax = [ax_cdf, ax_cmp_cit, ax_cmp_prd, ax_heat, ax_corr_cit, ax_corr_prd]

    plot_cdfs(
        ax=ax_cdf,
        l_cdf=tpl_rates_cmp,
        l_stages=(STAGE_EXAMPLE_CMP, STAGE_EXAMPLE_CMP + 1),
        l_labels=[f"$s_{STAGE_EXAMPLE_CMP + i}$" for i in range(2)],
        color_map=CM_CAREER_LENGTH)
    ax_cdf.set_xlabel("$R(s)$", labelpad=0.8)
    y_example_cmp = plot_rate_comparison(
        l_ax=(ax_cmp_cit, ax_cmp_prd),
        tpl_d_test=tpl_d_test_cmp,
        stat_test=test_cmp,
        plot_xticks=False)
    cb = plot_spearman_heatmap(tpl_rates_example=tpl_rates_cor, ax=ax_heat)
    y_example_corr = plot_rate_comparison(
        l_ax=(ax_corr_cit, ax_corr_prd),
        tpl_d_test=tpl_d_test_cor,
        stat_test=test_cor,
        plot_xticks=False)

    box_size_x = .2
    t_box_size_y = (
        .035 if test_cmp == MannWhitneyPermutTest else .08,
        .2 if test_cor == SpearmanPermutTest else .18)
    offset_y = (
        0,
        0.065)
    for x_box, y_box, ax_source, ax_target, box_size_y, offset_y in zip(
            (STAGE_EXAMPLE_CMP, STAGE_EXAMPLE_CORR),
            (y_example_cmp, y_example_corr),
            (ax_cmp_prd, ax_corr_prd),
            (ax_cdf, ax_heat),
            t_box_size_y,
            offset_y):
        draw_zooming_box(
            ax_source=ax_source,
            xy_source=(x_box - box_size_x / 2, y_box + offset_y - box_size_y / 2),
            size_box=(box_size_x, box_size_y)
        )
        draw_zooming_edge(
            fig=fig, xy_source=(x_box + box_size_x / 2, y_box + offset_y + box_size_y / 2),
            xy_target=(0,1), ax_source=ax_source, ax_target=ax_target
        )
        draw_zooming_edge(
            fig=fig, xy_source=(x_box + box_size_x / 2, y_box + offset_y - box_size_y / 2),
            xy_target=(0,0), ax_source=ax_source, ax_target=ax_target
        )

    fig.text(.1, .97, "A", fontdict={"weight": "bold"}, transform=fig.transFigure)
    fig.text(.1, .45, "B", fontdict={"weight": "bold"}, transform=fig.transFigure)
    for _x, metric in zip((.2, .52), TPL_STR_IMPACT):
        fig.text(_x, .97, metric, fontsize=10, transform=fig.transFigure)

    l_legends = []
    for metric, cm, pos_x in zip(TPL_STR_IMPACT, TPL_CM_IMPACT, (.045, 0.4)):
        legend = fig.legend(
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
            loc="upper left",
            bbox_to_anchor=(pos_x, 1.))
        l_legends.append(legend)

    for ax in (ax_corr_cit, ax_corr_prd):
        ax.set_xticks(
            np.arange(N_STAGES - 2),
            labels=map(lambda s: f"$s_{s} \\leftrightarrow s_{s+1}$",
                       np.arange(N_STAGES - 2)))
        ax.set_xlabel("career stage $s_i \\leftrightarrow s_{{i+1}}$")
    if test_cmp == MannWhitneyPermutTest:
        ax_cmp_cit.set_ylabel(r"$P(R_{s+1} > R_{s})$")
    cbar = fig.colorbar(cb, ax=ax_heat, location="right", fraction=.05)
    # l_legends.append(cbar)
    fig.subplots_adjust(wspace=1.5, hspace=.65)
    return fig, a_ax, l_legends

def plot_single_rate_result(
        t_ax: Tuple[plt.Axes, plt.Axes],
        t_d_tests: Tuple[pd.DataFrame, pd.DataFrame],
        stat_test: StatisticalTest):
    for ax, d_tests, color_map in zip(t_ax, t_d_tests, TPL_CM_IMPACT):
        gs_sm_tests = d_tests.groupby(["stage_max", "stage_curr", "stage_next"]).first()
        for stage_max in range(N_STAGES):
            a_s = []
            a_res = []
            a_ci = [[], []]
            for stage_curr in range(N_STAGES - 2):
                if (stage_max, stage_curr, stage_curr + 1) not in gs_sm_tests.index:
                    continue
                res = gs_sm_tests.loc[(stage_max, stage_curr, stage_curr + 1)]

                if res["p_value"] < .05:
                    a_s.append(stage_curr)
                    a_res.append(res["test_statistic"])
                    # a_ci[0].append(res["ci_low"])
                    # a_ci[1].append(res["ci_high"])
                    a_ci[0].append(np.abs(np.abs(res["test_statistic"]) - np.abs(res["ci_low"])))
                    a_ci[1].append(np.abs(np.abs(res["ci_high"]) - np.abs(res["test_statistic"])))

            ax.errorbar(
                a_s,
                a_res,
                yerr=a_ci,
                marker="o",
                color=color_map((stage_max+1) / N_STAGES),
                # label=f"$m={stage_max}$"
            )
        ax.axhline(
            y=stat_test.v_neutral, color="black", linestyle="dashed", zorder=0
        )
        ax.spines[["right", "top"]].set_visible(False)
    t_ax[0].set_ylabel(stat_test.label_y)

def plot_br_gender_comparison(
        tpl_d_tests_br_cmp_gender: Tuple[pd.DataFrame, pd.DataFrame],
        stat_test: StatisticalTest = MannWhitneyPermutTest
):
    fig,a_ax = plt.subplots(figsize=(WIDTH_FIG_PAPER, (2/3)*HEIGHT_FIG_PAPER), nrows=2, ncols=2, sharex=True, sharey="row")

    for a_ax_row, g in zip(
        a_ax,
        (GENDER_FEMALE, GENDER_MALE)):
        tpl_d_tests_br_gender_curr = (
            d_tests[d_tests["grouping_key"] == g.gender]
                for d_tests in tpl_d_tests_br_cmp_gender
        )
        plot_single_rate_result(
            t_ax=a_ax_row,
            t_d_tests=tpl_d_tests_br_gender_curr,
            stat_test=stat_test)

    l_legends = []
    for metric, cm, pos_x in zip(
            TPL_STR_IMPACT,
            TPL_CM_IMPACT,
            (.075, 0.525)):
        legend = fig.legend(
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
            loc="upper left",
            bbox_to_anchor=(pos_x, 1.05))
        legend.set_title(metric)
        l_legends.append(legend)

    l_legends.append(
        fig.text(.025, .95,
                 "women", transform=fig.transFigure))
    l_legends.append(
        fig.text(.025, .48,
                 "men", transform=fig.transFigure))
    for ax in a_ax[-1]:
        ax.set_xticks(
            np.arange(N_STAGES - 2),
            labels=map(
                lambda s: f"$s_{s} \\leftrightarrow s_{s+1}$",
                np.arange(N_STAGES - 2)))
        ax.set_xlabel("career stage $s_i \\leftrightarrow s_{{i+1}}$")

    # fig.subplots_adjust(wspace=.5, hspace=.6)

    return fig, a_ax, l_legends
