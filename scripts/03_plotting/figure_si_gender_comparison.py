import os
from typing import Dict, Any
from argparse import ArgumentParser

from cumulative_advantage_brokerage.config import parse_config
from cumulative_advantage_brokerage.constants import\
    ARG_POSTGRES_DB_APS, TPL_STR_IMPACT,\
    STR_CITATIONS, STR_PRODUCTIVITY, STR_CAREER_LENGTH,\
    ARG_PATH_CONTAINER_OUTPUT
from cumulative_advantage_brokerage.stats import\
    MannWhitneyPermutTest,\
    CollaboratorSeriesBrokerageComparison,\
    KolmogorovSmirnovPermutTest, SpearmanPermutTest
from cumulative_advantage_brokerage.dbm import\
    PostgreSQLEngine, CumAdvBrokSession, GENDER_FEMALE, GENDER_MALE
from cumulative_advantage_brokerage.queries import\
    get_bf_comparison_results_by_id, init_metric_id,\
    get_br_comparison_results_by_id
from cumulative_advantage_brokerage.visuals import\
    plot_bf_gender_comparison, plot_br_gender_comparison

MAP_TESTS = {
    MannWhitneyPermutTest.label_file: MannWhitneyPermutTest,
    KolmogorovSmirnovPermutTest.label_file: KolmogorovSmirnovPermutTest
}

def parse_args() -> Dict[str, Any]:
    ap = ArgumentParser()

    ap.add_argument("-idcmp-cit-bf-cmp", f"--id-bf-comparisons-{STR_CITATIONS}",
        default=None, type=int)
    ap.add_argument("-idcmp-prd-bf-cmp", f"--id-bf-comparisons-{STR_PRODUCTIVITY}",
        default=None, type=int)
    ap.add_argument("-idcmp-cit-br-cmp", f"--id-br-comparisons-{STR_CITATIONS}",
        default=None, type=int)
    ap.add_argument("-idcmp-prd-br-cmp", f"--id-br-comparisons-{STR_PRODUCTIVITY}",
        default=None, type=int)
    ap.add_argument("-idcmp-cit-br-cor", f"--id-br-correlations-{STR_CITATIONS}",
        default=None, type=int)
    ap.add_argument("-idcmp-prd-br-cor", f"--id-br-correlations-{STR_PRODUCTIVITY}",
        default=None, type=int)

    d_a = vars(ap.parse_args())

    return d_a

def main():
    config = parse_config([ARG_POSTGRES_DB_APS])
    engine = PostgreSQLEngine.from_config(config, key_dbname=ARG_POSTGRES_DB_APS)
    args = parse_args()

    tpl_d_tests = []
    tpl_str_files = []
    with CumAdvBrokSession(engine) as session:
        for key_cmp, f_get_test_results in zip(
                ("id_bf_comparisons_", "id_br_comparisons_", "id_br_correlations_"),
                (get_bf_comparison_results_by_id, get_br_comparison_results_by_id, get_br_comparison_results_by_id)):
            print(f"Comparison {key_cmp[3:-1]}")
            tpl_d_metrics = []
            for str_metric,  in zip(
                    TPL_STR_IMPACT):
                print(f"\tWorking on `{str_metric}`.")
                id_metric_test = args[f"{key_cmp}{str_metric}"]
                if id_metric_test is None:
                    id_metric_test = init_metric_id(
                        dict(metric=str_metric,
                            type=CollaboratorSeriesBrokerageComparison.__name__),
                        session)
                print(f"\tGetting test results for ID=`{id_metric_test}`...")
                tpl_d_metrics.append(f_get_test_results(
                        id_metric_config=id_metric_test,
                        metric=str_metric,
                        session=session))
            tpl_d_tests.append(tpl_d_metrics)
            tpl_str_files.append(os.path.join(
                config[ARG_PATH_CONTAINER_OUTPUT], f"si_gender_{key_cmp[3:-1]}.pdf"))

    fig, _, l_legends = plot_bf_gender_comparison(
        tpl_d_tests_bf_cmp_gender=tpl_d_tests[0],
    )
    print(f"Saving figure to {tpl_str_files[0]}")
    fig.savefig(
        tpl_str_files[0],
        bbox_inches='tight',
        bbox_extra_artists=l_legends)

    fig, a_ax, l_legends = plot_br_gender_comparison(
        tpl_d_tests_br_cmp_gender=tpl_d_tests[1],
        stat_test=MannWhitneyPermutTest
    )
    for ax in a_ax[:,0]:
        ax.set_ylabel(r"$P(R_{s+1} > R_s)$")
    print(f"Saving figure to {tpl_str_files[1]}")
    fig.savefig(
        tpl_str_files[1],
        bbox_inches='tight',
        bbox_extra_artists=l_legends)

    fig, _, l_legends = plot_br_gender_comparison(
        tpl_d_tests_br_cmp_gender=tpl_d_tests[2],
        stat_test=SpearmanPermutTest
    )
    print(f"Saving figure to {tpl_str_files[2]}")
    fig.savefig(
        tpl_str_files[2],
        bbox_inches='tight',
        bbox_extra_artists=l_legends)

if __name__ == "__main__":
    main()
