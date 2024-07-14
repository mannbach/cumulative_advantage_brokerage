import os
from typing import Dict, Any
from argparse import ArgumentParser

from cumulative_advantage_brokerage.config import parse_config
from cumulative_advantage_brokerage.constants import\
    ARG_POSTGRES_DB_APS, TPL_STR_IMPACT,\
    STR_CITATIONS, STR_PRODUCTIVITY, STR_CAREER_LENGTH,\
    ARG_PATH_CONTAINER_OUTPUT
from cumulative_advantage_brokerage.career_series import\
    CollaboratorSeriesBrokerageInference,\
    ImpactGroupsInference
from cumulative_advantage_brokerage.stats import\
    MannWhitneyPermutTest,\
    CollaboratorSeriesBrokerageComparison,\
    GrouperDummy
from cumulative_advantage_brokerage.dbm import\
    PostgreSQLEngine, CumAdvBrokSession,\
    get_bf_comparison_results_by_id, init_metric_id
from cumulative_advantage_brokerage.visuals import\
    plot_brokerage_frequency_comparison,\
    STAGE_MAX_DIFF_HIGH, STAGE_MAX_DIFF_LOW, STAGE_EXAMPLE

def parse_args() -> Dict[str, Any]:
    ap = ArgumentParser()
    ap.add_argument(
        "-id-cs", "--id-collaborator-series",
        default=None, type=int)
    ap.add_argument("-idig-cit", f"--id-impact-group-{STR_CITATIONS}",
        default=None, type=int)
    ap.add_argument("-idig-prd", f"--id-impact-group-{STR_PRODUCTIVITY}",
        default=None, type=int)
    ap.add_argument("-idcmp-cit", f"--id-comparisons-{STR_CITATIONS}",
        default=None, type=int)
    ap.add_argument("-idcmp-prd", f"--id-comparisons-{STR_PRODUCTIVITY}",
        default=None, type=int)

    d_a = vars(ap.parse_args())

    return d_a

def main():
    config = parse_config([ARG_POSTGRES_DB_APS])
    engine = PostgreSQLEngine.from_config(config, key_dbname=ARG_POSTGRES_DB_APS)
    args = parse_args()

    file_out = os.path.join(
        config[ARG_PATH_CONTAINER_OUTPUT], "03_brokerage_frequency_comparison.pdf")

    tpl_d_test = []
    tpl_a_cdf = []
    with CumAdvBrokSession(engine) as session:
        for str_metric, ig_sample in zip(
                TPL_STR_IMPACT,
                (STAGE_MAX_DIFF_HIGH, STAGE_MAX_DIFF_LOW)):
            print(f"Working on `{str_metric}`...\nGetting test results.")
            id_metric_test = args[f"id_comparisons_{str_metric}"]
            if id_metric_test is None:
                id_metric_test = init_metric_id(
                    dict(metric=str_metric,
                         type=CollaboratorSeriesBrokerageComparison.__name__),
                    session)
            tpl_d_test.append(
                get_bf_comparison_results_by_id(
                    id_metric_config=id_metric_test,
                    metric=str_metric,
                    session=session))
            print(f"\tResults:\n{tpl_d_test[-1]}")

            print("Getting sample career series...")
            id_metric_series = args["id_collaborator_series"]
            if id_metric_series is None:
                id_metric_series = init_metric_id(
                    metric_args=dict(
                        metric=STR_CAREER_LENGTH,
                        type=CollaboratorSeriesBrokerageInference.__name__),
                    session=session)
            id_metric_ig = args[f"id_impact_group_{str_metric}"]
            if id_metric_ig is None:
                id_metric_ig = init_metric_id(
                    metric_args=dict(
                        metric=str_metric,
                        type=ImpactGroupsInference.__name__),
                    session=session)
            print(f"\tQuerying with metric IDs:\n\tCareer - {id_metric_series}\n\tImpact group - {id_metric_ig}")
            cmp = CollaboratorSeriesBrokerageComparison(
                session=session,
                id_metric_config_comparison=None,
                id_metric_config_career=id_metric_series,
                id_metric_config_impact_group=id_metric_ig,
                statistical_test=None,
                grouper=GrouperDummy)
            # cmp.init_cached_data()
            tpl_cdf = tuple(
                cmp.get_values(
                    stage_curr=STAGE_EXAMPLE,
                    stage_max=ig_sample + i,
                    grouping_key=GrouperDummy.possible_values[0])[1]\
                        for i in range(2))
            tpl_a_cdf.append(tpl_cdf)

    fig = plot_brokerage_frequency_comparison(
        tpl_d_test=tpl_d_test,
        tpl_a_cdf=tpl_a_cdf,
        stat_test=MannWhitneyPermutTest
    )
    print(f"Saving figure to {file_out}...")
    fig.savefig(file_out, bbox_inches='tight')

if __name__ == "__main__":
    main()
