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
    CollaboratorSeriesRateStageComparison,\
    CollaboratorSeriesRateStageCorrelation,\
    GrouperDummy, ContKolmogorovSmirnovPermutTest,\
    MannWhitneyPermutTest, SpearmanPermutTest,\
    PearsonPermutTest
from cumulative_advantage_brokerage.dbm import\
    PostgreSQLEngine, CumAdvBrokSession
from cumulative_advantage_brokerage.queries import\
    get_br_comparison_results_by_id, init_metric_id,\
    get_bin_values_by_id
from cumulative_advantage_brokerage.visuals import\
    plot_brokerage_rate_comparison,\
    STAGE_EXAMPLE_CMP, STAGE_EXAMPLE_CORR,\
    STAGE_MAX_EXAMPLE_CMP, STAGE_MAX_EXAMPLE_CORR

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
    ap.add_argument("-idcor-cit", f"--id-correlation-{STR_CITATIONS}",
        default=None, type=int)
    ap.add_argument("-idcor-prd", f"--id-correlation-{STR_PRODUCTIVITY}",
        default=None, type=int)

    ap.add_argument("--alt-metrics", action="store_true", default=False)

    d_a = vars(ap.parse_args())

    return d_a

def main():
    config = parse_config([ARG_POSTGRES_DB_APS])
    engine = PostgreSQLEngine.from_config(config, key_dbname=ARG_POSTGRES_DB_APS)
    args = parse_args()

    tpl_d_cmp = []
    tpl_d_cor = []
    tpl_rates_cmp = []
    tpl_rates_cor = []
    with CumAdvBrokSession(engine) as session:
        for Comparison, ig_sample, stage_sample, tpl_d, tpl_r, key_cmp in zip(
                (
                    CollaboratorSeriesRateStageComparison, CollaboratorSeriesRateStageCorrelation),
                (STAGE_MAX_EXAMPLE_CMP, STAGE_MAX_EXAMPLE_CORR),
                (STAGE_EXAMPLE_CMP, STAGE_EXAMPLE_CORR),
                (tpl_d_cmp, tpl_d_cor),
                (tpl_rates_cmp, tpl_rates_cor),
                ("id_comparisons_", "id_correlation_")):
            for str_metric in TPL_STR_IMPACT:
                print(f"Working on `{str_metric}`...\nGetting test results.")
                id_metric_test = args[f"{key_cmp}{str_metric}"]
                if id_metric_test is None:
                    id_metric_test = init_metric_id(
                        dict(metric=str_metric,
                            type=Comparison.__name__),
                        session)
                tpl_d.append(
                    get_br_comparison_results_by_id(
                        id_metric_config=id_metric_test,
                        metric=str_metric,
                        session=session))
                print(f"\tID `{id_metric_test}` results:\n{tpl_d[-1]}")

            print("Getting sample career series...")
            id_metric_series = args["id_collaborator_series"]
            if id_metric_series is None:
                id_metric_series = init_metric_id(
                    metric_args=dict(
                        metric=STR_CAREER_LENGTH,
                        type=CollaboratorSeriesBrokerageInference.__name__),
                    session=session)
            id_metric_ig = args[f"id_impact_group_{STR_PRODUCTIVITY}"]
            if id_metric_ig is None:
                id_metric_ig = init_metric_id(
                    metric_args=dict(
                        metric=STR_PRODUCTIVITY,
                        type=ImpactGroupsInference.__name__),
                    session=session)

            print((f"\tQuerying with metric IDs:\n"
                   f"\t\tCareer - {id_metric_series}\n"
                   f"\t\tImpact group - {id_metric_ig}"))
            cmp = Comparison(
                session=session,
                id_metric_config_comparison=None,
                id_metric_config_career=id_metric_series,
                id_metric_config_impact_group=id_metric_ig,
                bins=get_bin_values_by_id(
                    session=session,
                    id_config=id_metric_series),
                statistical_test=None,
                grouper=GrouperDummy)

            _t_idc = []
            for i in range(2):
                a_idc, a_rates = cmp.get_values(
                        stage_curr=stage_sample + i,
                        stage_max=ig_sample,
                        grouping_key=GrouperDummy.possible_values[0])
                tpl_r.append(a_rates)
                _t_idc.append(a_idc)

            if isinstance(cmp, CollaboratorSeriesRateStageCorrelation):
                a_rates_curr, a_rates_next = cmp.align_values(
                    a_vals_stage_curr=tpl_r[0],
                    a_vals_stage_next=tpl_r[1],
                    a_idc_curr=_t_idc[0],
                    a_idc_next=_t_idc[1])
                tpl_r[0] = a_rates_curr
                tpl_r[1] = a_rates_next

    file_out = os.path.join(
        config[ARG_PATH_CONTAINER_OUTPUT],
        "04_brokerage_rate_comparison.pdf" if not args["alt_metrics"] else "si_brokerage_rate_ks_prs_comparison.pdf")
    fig = plot_brokerage_rate_comparison(
        tpl_d_test_cmp=tpl_d_cmp,
        tpl_d_test_cor=tpl_d_cor,
        tpl_rates_cmp=tpl_rates_cmp,
        tpl_rates_cor=tpl_rates_cor,
        test_cmp=ContKolmogorovSmirnovPermutTest if args["alt_metrics"] else MannWhitneyPermutTest,
        test_cor=PearsonPermutTest if args["alt_metrics"] else SpearmanPermutTest
    )
    print(f"Saving figure to {file_out}...")
    fig.savefig(file_out, bbox_inches='tight')

if __name__ == "__main__":
    main()
