from typing import List, Tuple, Union, Type

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from .visuals import draw_zooming_box, draw_zooming_edge, plot_cdfs
from ..stats import StatisticalTest, GrouperDummy, Grouper, MannWhitneyPermutTest, GrouperGender, GrouperBirthDecade
from ..constants import OFFSET_MARKERS, N_STAGES, TPL_STR_IMPACT, TPL_CM_IMPACT, CM_CITATION, CM_PRODUCTIVITY, WIDTH_FIG_PAPER, HEIGHT_FIG_PAPER, M_GENDER_LABEL
from ..dbm.models.gender import GENDER_FEMALE, GENDER_MALE, GENDER_UNKNOWN

STAGE_MAX_DIFF_HIGH = 3 # Comparison 3 <-> 4
STAGE_MAX_DIFF_LOW = 1
STAGE_EXAMPLE = 3
TPL_METRIC_EXAMPLE = TPL_STR_IMPACT

def plot_test_results(
        a_ax: List[plt.Axes],
        stat_test: StatisticalTest,
        tpl_d_test: Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame],
        grouper: Grouper = GrouperDummy,
        show_not_sign: bool = False,
        ret_sample: bool = True,
        l_cols_labels: Union[None, List[str]] = None)\
            -> Tuple[float, float]:
    t_y_example = [0.5,0.5]
    _l_cols_labels = grouper.possible_values
    _l_grouping_keys = grouper.possible_values if len(grouper.possible_values) > 0 else [None]
    if l_cols_labels is not None:
        _l_cols_labels = l_cols_labels
    elif len(_l_cols_labels) == 0:
        _l_cols_labels = [""]

    for ax_metric, metric, d_test, color_map in zip(a_ax, TPL_STR_IMPACT, tpl_d_test, TPL_CM_IMPACT):
        print(f"metric: {metric}")

        # Apply multi index by grouping
        _gs_tests = d_test.groupby(
            ["max_stage_curr","max_stage_next","grouping_key","stage"] if len(grouper.possible_values) > 0 else\
                ["max_stage_curr","max_stage_next", "stage"])\
                    .first()

        # Iterate over groups as columns
        for ax_col, col, _col_label in zip(ax_metric, _l_grouping_keys, _l_cols_labels):
            print(f"\tcolumn: {_col_label}")
            ax_col.axhline(stat_test.v_neutral, color="black", linestyle="dashed")

            for stage_target_curr, _off in zip(range(N_STAGES - 1), (-(3/2)*OFFSET_MARKERS, -OFFSET_MARKERS/2, OFFSET_MARKERS/2, (3/2)*OFFSET_MARKERS)):
                print(f"\t\tstage_max: {stage_target_curr}")
                _color= color_map((stage_target_curr+1) / N_STAGES)

                stage_target_next = stage_target_curr + 1

                _idx = (stage_target_curr, stage_target_next, col) if col is not None else (stage_target_curr, stage_target_next)
                if _idx not in _gs_tests.index:
                    continue

                d_test_sel = _gs_tests.loc[_idx].sort_index()
                if not show_not_sign:
                    d_test_sel = d_test_sel[d_test_sel["p_value"] < .05]
                if len(d_test_sel) == 0:
                    continue

                _alphas= [1. if p <.05 else .3\
                            for p in d_test_sel["p_value"] if p < 0.05 or show_not_sign]

                if ret_sample:
                    if (stage_target_curr == STAGE_MAX_DIFF_HIGH) and (metric == TPL_STR_IMPACT[0]):
                        t_y_example[0] = d_test_sel["test_statistic"].loc[STAGE_EXAMPLE]
                    if (stage_target_curr == STAGE_MAX_DIFF_LOW) and (metric == TPL_STR_IMPACT[1]):
                        t_y_example[1] = d_test_sel.loc[STAGE_EXAMPLE, "test_statistic"]

                ax_col.scatter(
                    d_test_sel.index.values + _off,
                    d_test_sel["test_statistic"],
                    color=_color,
                    marker="o",
                    alpha=_alphas)
                for x,y,l,h,a in zip(d_test_sel.index.values, d_test_sel["test_statistic"],d_test_sel["ci_low"], d_test_sel["ci_high"], _alphas):
                    if None in (l,h):
                        continue
                    ax_col.errorbar(
                        [x + _off], [y],
                        yerr=([np.abs(np.abs(y)-np.abs(l))],[np.abs(np.abs(h)-np.abs(y))]),
                        color=_color,
                        alpha=a,
                        fmt="none")

            ax_col.spines[["top", "right"]].set_visible(False)

    for ax in a_ax[-1]:
        ax.set_xlabel("career stage $s_i$")
        # ticks = ax.get_xticks()
        # ax.set_xticks(ticks=[int(x) for x in ticks])
        ax.set_xticks(ticks=range(N_STAGES-1), labels=[f"$s_{s}$" for s in range(N_STAGES - 1)])
    if len(_l_cols_labels) > 1:
        for ax, col in zip(a_ax[0], _l_cols_labels):
            ax.set_title(f"{col}", fontsize=10)
    for ax, metric in zip(a_ax[:,0], TPL_STR_IMPACT):
        _=ax.set_ylabel(stat_test.label_y)
        # _=ax.text(.05, .9, metric, fontsize=10, transform=ax.transAxes)
    if ret_sample:
        return tuple(t_y_example)

def plot_brokerage_frequency_comparison(
        tpl_d_test: Tuple[pd.DataFrame, pd.DataFrame],
        tpl_a_cdf: Tuple[np.ndarray, np.ndarray],
        stat_test: Type[StatisticalTest] = MannWhitneyPermutTest) -> plt.Figure:

    # Create a figure and a grid layout
    fig = plt.figure(
        figsize=[
            WIDTH_FIG_PAPER,
            (2/3)*HEIGHT_FIG_PAPER])  # Adjust the figure size as needed
    grid = plt.GridSpec(4, 3)

    # Define the subplots using the grid layout
    left_bottom_subplot = fig.add_subplot(
        grid[2:, :2])  # Combine two rows and span 75% of the width
    left_top_subplot = fig.add_subplot(
        grid[:2, :2],
        sharex=left_bottom_subplot,
        sharey=left_bottom_subplot)  # Combine two rows and span 75% of the width
    right_top_subplot = fig.add_subplot(grid[:2, 2])  # Cover the remaining 25% and first row
    right_bottom_subplot = fig.add_subplot(
        grid[2:, 2], sharey=right_top_subplot)  # Cover the remaining 25% and second row

    l_y_example = plot_test_results(
        tpl_d_test=tpl_d_test,
        a_ax=np.asarray([[left_top_subplot], [left_bottom_subplot]]),
        grouper=GrouperDummy,
        stat_test=stat_test,
        show_not_sign=False)

    plot_cdfs(
        ax=right_top_subplot,
        l_cdf=tpl_a_cdf[0],
        l_stages=(STAGE_MAX_DIFF_HIGH, STAGE_MAX_DIFF_HIGH+1),
        l_labels=[f"$Q_{STAGE_MAX_DIFF_HIGH+i}$" for i in range(2)],
        color_map=CM_CITATION,
    )
    plot_cdfs(
        ax=right_bottom_subplot,
        l_cdf=tpl_a_cdf[1],
        l_stages=(STAGE_MAX_DIFF_LOW, STAGE_MAX_DIFF_LOW+1),
        l_labels=[f"$Q_{STAGE_MAX_DIFF_LOW+i}$" for i in range(2)],
        color_map=CM_PRODUCTIVITY,
    )

    box_size_x, box_size_y = .25, .06
    for stage_max, x_box, y_box, ax_source, ax_target in zip(
        (STAGE_MAX_DIFF_HIGH, STAGE_MAX_DIFF_LOW),
        (
            STAGE_EXAMPLE + ((STAGE_MAX_DIFF_HIGH/2)*OFFSET_MARKERS),
            STAGE_EXAMPLE - ((STAGE_MAX_DIFF_LOW/2)*OFFSET_MARKERS)),
        l_y_example,
        (left_top_subplot, left_bottom_subplot),
        (right_top_subplot, right_bottom_subplot)):

        draw_zooming_box(
            ax_source=ax_source,
            xy_source=(x_box - box_size_x / 2, y_box - box_size_y / 2),
            size_box=(box_size_x, box_size_y)
        )
        draw_zooming_edge(
            fig=fig, xy_source=(x_box + box_size_x / 2, y_box + box_size_y / 2),
            xy_target=(0,1), ax_source=ax_source, ax_target=ax_target
        )
        draw_zooming_edge(
            fig=fig, xy_source=(x_box + box_size_x / 2, y_box - box_size_y / 2),
            xy_target=(0,0), ax_source=ax_source, ax_target=ax_target
        )

    l_legends = []
    for metric, cm, pos_y in zip(TPL_STR_IMPACT, TPL_CM_IMPACT, (.97, .5)):
        legend = fig.legend(
            [plt.Line2D([], [],
                color=cm((stage_max+1) / N_STAGES),
                label=stage_max,
                marker="o",
                linestyle="None")\
                    for stage_max in range(N_STAGES - 1)],
            [f"$Q_{m} \\leftrightarrow Q_{m+1}$" for m in range(N_STAGES - 1)],
            frameon=False,
            ncol=N_STAGES - 1,
            columnspacing=.1,
            handletextpad=.05,
            loc="upper center",
            bbox_to_anchor=(0.36, pos_y))
        fig.text(.4, pos_y-.02, metric, transform=fig.transFigure)
        l_legends.append(legend)
    for ax in (right_top_subplot, right_bottom_subplot):
        legend=ax.legend(frameon=False, handlelength=1, loc="lower right")
        l_legends.append(legend)
    fig.text(.1, .95, "A", fontdict={"weight": "bold"}, transform=fig.transFigure)
    fig.text(.1, .47, "B", fontdict={"weight": "bold"}, transform=fig.transFigure)
    fig.subplots_adjust(hspace=5.5, wspace=.5)

    return fig

def plot_bf_gender_comparison(
        tpl_d_tests_bf_cmp_gender
):
    fig, a_ax = plt.subplots(
        figsize=[WIDTH_FIG_PAPER, (3/5)*HEIGHT_FIG_PAPER],
        nrows=2, ncols=2, sharex=True, sharey=True)

    plot_test_results(
        a_ax=a_ax,
        grouper=GrouperGender,
        stat_test=MannWhitneyPermutTest,
        tpl_d_test=tpl_d_tests_bf_cmp_gender,
        show_not_sign=False,
        ret_sample=False)

    l_legends = []
    for metric, cm, pos_y in zip(TPL_STR_IMPACT, TPL_CM_IMPACT, (.97, .53)):
        legend = fig.legend(
            [plt.Line2D([], [],
                color=cm((stage_max+1) / N_STAGES),
                label=stage_max,
                marker="o",
                linestyle="None")\
                    for stage_max in range(N_STAGES - 1)],
            [f"$Q_{m} \\leftrightarrow Q_{m+1}$" for m in range(N_STAGES - 1)],
            frameon=False,
            ncol=N_STAGES - 1,
            columnspacing=.1,
            handletextpad=.05,
            loc="upper center",
            bbox_to_anchor=(0.5, pos_y))
        # legend.set_title(metric)
        fig.text(.45, pos_y-.02, metric, transform=fig.transFigure)
        l_legends.append(legend)

    for ax in a_ax[0]:
        ax.set_title(None)
    for g, ax in zip((GENDER_FEMALE, GENDER_MALE), a_ax[0,:]):
        ax.text(.08, .8, M_GENDER_LABEL[g.gender], transform=ax.transAxes)

    fig.subplots_adjust(hspace=.7, wspace=.1)
    return fig, a_ax, l_legends

def plot_bf_test_results_dec(
    tpl_fig_ax: Tuple[plt.Figure, List[plt.Axes]],
    d_test: pd.DataFrame,
    color_map: str,
    show_not_sign: bool = False,
    print_stages: bool = True
):
    _, a_ax = tpl_fig_ax

    _gs_tests = d_test.groupby(
            ["stage", "max_stage_curr","max_stage_next","grouping_key"])\
        .first()

    for stage, ax_row in enumerate(a_ax):
        print(f"\tstage: {stage}")
        ax_row.axhline(
            MannWhitneyPermutTest.v_neutral,
            color="black", linestyle="dashed")

        for stage_target_curr, _off in zip(
                range(N_STAGES - 1),
                (-(3/2)*OFFSET_MARKERS, -OFFSET_MARKERS/2, OFFSET_MARKERS/2, (3/2)*OFFSET_MARKERS)):
            print(f"\t\tstage_max: {stage_target_curr}")
            _color= color_map((stage_target_curr+1) / N_STAGES)
            _off *= 10

            stage_target_next = stage_target_curr + 1

            _idx = (stage, stage_target_curr, stage_target_next)
            if _idx not in _gs_tests.index:
                continue

            d_test_sel = _gs_tests.loc[_idx].sort_index()
            if not show_not_sign:
                d_test_sel = d_test_sel[d_test_sel["p_value"] < .05]
            if len(d_test_sel) == 0:
                continue

            _alphas= [1. if p <.05 else .3\
                        for p in d_test_sel["p_value"] if p < 0.05 or show_not_sign]

            ax_row.scatter(
                [int(dec) * 10 + _off for dec in d_test_sel.index.values],
                d_test_sel["test_statistic"],
                color=_color,
                marker="o",
                alpha=_alphas)
            for x,y,l,h,a in zip(
                    d_test_sel.index.values,
                    d_test_sel["test_statistic"],
                    d_test_sel["ci_low"],
                    d_test_sel["ci_high"],
                    _alphas):
                if None in (l,h):
                    continue
                ax_row.errorbar(
                    [int(x) * 10 + _off], [y],
                    yerr=([np.abs(np.abs(y)-np.abs(l))],[np.abs(np.abs(h)-np.abs(y))]),
                    color=_color,
                    alpha=a,
                    fmt="none")

        ax_row.spines[["top", "right"]].set_visible(False)

    if print_stages:
        for stage, ax in enumerate(a_ax):
            ax.text(.075, .9, f"$s_{stage}$", fontsize=10, transform=ax.transAxes)
    a_ax[-1].set_xlabel("Decade of first publication")

def plot_birth_decade_comparison(
    tpl_d_tests_bf_cmp: Tuple[pd.DataFrame, pd.DataFrame]
):
    fig, a_ax = plt.subplots(
        nrows=N_STAGES - 1,
        ncols=2,
        sharex=True,
        sharey=True,
        figsize=(WIDTH_FIG_PAPER, HEIGHT_FIG_PAPER))
    l_artists = []

    plot_bf_test_results_dec(
        tpl_fig_ax=(fig,a_ax[:,0]),
        d_test=tpl_d_tests_bf_cmp[0],
        color_map=TPL_CM_IMPACT[0],
    )

    plot_bf_test_results_dec(
        tpl_fig_ax=(fig,a_ax[:,1]),
        d_test=tpl_d_tests_bf_cmp[1],
        color_map=TPL_CM_IMPACT[1],
        print_stages=False
    )

    for metric, pos_x, color_map in zip(TPL_STR_IMPACT, (.15, .6), TPL_CM_IMPACT):
        legend = fig.legend(
            [plt.Line2D([], [],
                color=color_map((stage_max+1) / N_STAGES),
                label=stage_max,
                marker="o",
                linestyle="None")\
                    for stage_max in range(N_STAGES - 1)],
            [f"$Q_{m} \\leftrightarrow Q_{m+1}$" for m in range(N_STAGES - 1)],
            frameon=False,
            ncol=2,
            columnspacing=.1,
            handletextpad=.05,
            loc="center left",
            bbox_to_anchor=(pos_x, .96),
            title=metric)
        legend.get_title().set_fontsize(10)
        l_artists.append(legend)
    t=fig.text(.05, .4, MannWhitneyPermutTest.label_y, rotation=90, transform=fig.transFigure)
    l_artists.append(t)

    fig.subplots_adjust(hspace=.5, wspace=.1)

    return fig, a_ax, l_artists
