import os
from typing import Dict, Any
from argparse import ArgumentParser
from itertools import product

import pandas as pd
import numpy as np

from cumulative_advantage_brokerage.stats import\
    MannWhitneyPermutTest,\
    CollaboratorSeriesBrokerageComparison,\
    CollaboratorSeriesRateStageComparison,\
    GrouperDummy
from cumulative_advantage_brokerage.config import parse_config
from cumulative_advantage_brokerage.constants import\
    ARG_POSTGRES_DB_APS,\
    STR_CAREER_LENGTH,\
    ARG_PATH_CONTAINER_OUTPUT, N_STAGES,\
    L_MOTIF_GEN_SORTED_AGG, D_MOTIF_GEN_SORTED_AGG_MISSING,\
    STR_CITATIONS, STR_PRODUCTIVITY,\
    TPL_CM_IMPACT, CM_CAREER_LENGTH, TPL_STR_IMPACT
from cumulative_advantage_brokerage.career_series import\
    CollaboratorSeriesBrokerageInference, ImpactGroupsInference
from cumulative_advantage_brokerage.dbm import\
    GENDER_FEMALE, GENDER_MALE, GENDER_UNKNOWN
from cumulative_advantage_brokerage.queries import\
    init_metric_id, get_brokerage_freq_by_id,\
    get_bin_values_by_id, get_auth_info, get_brokerage_events
from cumulative_advantage_brokerage.dbm import\
    PostgreSQLEngine, CumAdvBrokSession
from cumulative_advantage_brokerage.visuals import\
    plot_gender_seniority,\
    plot_author_gender_evolution,\
    plot_gen_brok_evolution,\
    setup_ccdf_plot,\
    plot_cdfs


def parse_args() -> Dict[str, Any]:
    ap = ArgumentParser()
    ap.add_argument(
        "-id-cs", "--id-collaborator-series",
        default=None, type=int)
    ap.add_argument("-idig-cit", f"--id-impact-group-{STR_CITATIONS}",
        default=None, type=int)
    ap.add_argument("-idig-prd", f"--id-impact-group-{STR_PRODUCTIVITY}",
        default=None, type=int)

    d_a = vars(ap.parse_args())

    return d_a

def main():
    config = parse_config([ARG_POSTGRES_DB_APS])
    engine = PostgreSQLEngine.from_config(config, key_dbname=ARG_POSTGRES_DB_APS)
    args = parse_args()

    file_out_frequencies = os.path.join(
        config[ARG_PATH_CONTAINER_OUTPUT], "si_bf_ccdfs.pdf")

    file_out_rates = os.path.join(
        config[ARG_PATH_CONTAINER_OUTPUT], "si_br_ccdfs.pdf")

    l_vals_bf, l_vals_br = [], []
    with CumAdvBrokSession(engine) as session:
        id_metric_series = args["id_collaborator_series"]
        if id_metric_series is None:
            id_metric_series = init_metric_id(
                metric_args=dict(
                    metric=STR_CAREER_LENGTH,
                    type=CollaboratorSeriesBrokerageInference.__name__),
                session=session)

        for Comparison, l_vals in zip(
                (CollaboratorSeriesBrokerageComparison, CollaboratorSeriesRateStageComparison),
                (l_vals_bf, l_vals_br)):
            for str_metric in TPL_STR_IMPACT:
                id_metric_ig = args[f"id_impact_group_{str_metric}"]
                if id_metric_ig is None:
                    id_metric_ig = init_metric_id(
                        metric_args=dict(
                            metric=str_metric,
                            type=ImpactGroupsInference.__name__),
                        session=session)
                cmp_args = dict(
                    session=session,
                    id_metric_config_comparison=None,
                    id_metric_config_career=id_metric_series,
                    id_metric_config_impact_group=id_metric_ig,
                    statistical_test=None,
                    grouper=GrouperDummy)
                if Comparison == CollaboratorSeriesRateStageComparison:
                    cmp_args["bins"] = get_bin_values_by_id(
                        session=session,
                        id_config=id_metric_series)
                cmp = Comparison(**cmp_args)
                cmp.init_cached_data()
                print(f"Setting up comparison {Comparison.__name__}.\nQuerying values...")

                if Comparison == CollaboratorSeriesBrokerageComparison:
                    for stage in range(N_STAGES - 1):
                        print(f"\tStage {stage}")
                        l_vals_stage = []
                        for ig in range(N_STAGES):
                            print(f"\tIG {ig}")
                            l_vals_stage.append(cmp.get_values(
                                stage_curr=stage,
                                stage_max=ig,
                                grouping_key=GrouperDummy.possible_values[0])[1])
                        l_vals.append(l_vals_stage)
                else:
                    for ig in range(N_STAGES):
                        print(f"\tIG {ig}")
                        l_vals_ig = []
                        for stage in range(N_STAGES - 1):
                            print(f"\tStage {stage}")
                            l_vals_ig.append(cmp.get_values(
                                stage_curr=stage,
                                stage_max=ig,
                                grouping_key=GrouperDummy.possible_values[0])[1])
                        l_vals.append(l_vals_ig)


    fig_bf, ax_bf, l_legends_bf = setup_ccdf_plot(col_stages=True, xlabel="$B(s_i)$")
    fig_br, ax_br, l_legends_br = setup_ccdf_plot(col_stages=False, xlabel="$R(s_i)$")

    for str_metric, ax_metric_bf, ax_metric_br, color_map in zip(
            TPL_STR_IMPACT, ax_bf, ax_br, TPL_CM_IMPACT):
        for ig, ax_ig, l_bf_ig in zip(
                range(N_STAGES-1),ax_metric_bf,l_vals_bf):
            plot_cdfs(
                ax=ax_ig,
                l_cdf=l_bf_ig,
                l_stages=range(N_STAGES),
                l_labels=[f"Q_{stage}" for stage in range(N_STAGES)],
                color_map=color_map,
                ccdf=True,
                print_ylabel=False
            )
        for stage, ax_stage, l_br_stage in zip(
                range(N_STAGES), ax_metric_br, l_vals_br):
            plot_cdfs(
                ax=ax_stage,
                l_cdf=l_br_stage,
                l_stages=range(N_STAGES - 1),
                l_labels=[f"s_{stage}" for stage in range(N_STAGES - 1)],
                color_map=CM_CAREER_LENGTH,
                ccdf=True,
                print_ylabel=False
            )
    fig_br.text(.225, .9085, "career stages")

    print(f"Saving frequency CCDFs to {file_out_frequencies}.")
    fig_bf.savefig(file_out_frequencies,
        bbox_inches="tight",
        bbox_extra_artists=l_legends_bf)

    print(f"Saving rate CCDFs to {file_out_rates}.")
    fig_br.savefig(file_out_rates, bbox_inches="tight",
        bbox_extra_artists=l_legends_br)

if __name__ == "__main__":
    main()
