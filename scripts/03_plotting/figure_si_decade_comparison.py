import os
from typing import Dict, Any
from argparse import ArgumentParser

from cumulative_advantage_brokerage.config import parse_config
from cumulative_advantage_brokerage.constants import\
    ARG_POSTGRES_DB_APS, TPL_STR_IMPACT,\
    STR_CITATIONS, STR_PRODUCTIVITY,\
    ARG_PATH_CONTAINER_OUTPUT
from cumulative_advantage_brokerage.stats import\
    CollaboratorSeriesBrokerageComparison, MannWhitneyPermutTest
from cumulative_advantage_brokerage.dbm import\
    PostgreSQLEngine, CumAdvBrokSession
from cumulative_advantage_brokerage.queries import\
    get_bf_comparison_results_by_id, init_metric_id
from cumulative_advantage_brokerage.visuals import\
    plot_birth_decade_comparison

def parse_args() -> Dict[str, Any]:
    ap = ArgumentParser()

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
        config[ARG_PATH_CONTAINER_OUTPUT], "si_brokerage_frequency_decade_comparison.pdf")

    tpl_d_test = []
    with CumAdvBrokSession(engine) as session:
        for str_metric,  in zip(
                TPL_STR_IMPACT):
            print(f"Working on `{str_metric}`...\nGetting test results.")
            id_metric_test = args[f"id_comparisons_{str_metric}"]
            if id_metric_test is None:
                id_metric_test = init_metric_id(
                    dict(metric=str_metric,
                         type=CollaboratorSeriesBrokerageComparison.__name__),
                    session)
            tpl_d_test.append(get_bf_comparison_results_by_id(
                    id_metric_config=id_metric_test,
                    metric=str_metric,
                    session=session))

    fig, _, l_artists = plot_birth_decade_comparison(
        tpl_d_tests_bf_cmp=tpl_d_test,
    )

    print(f"Saving figure to {file_out}...")
    fig.savefig(
        file_out,
        bbox_inches='tight',
        bbox_extra_artists=l_artists)

if __name__ == "__main__":
    main()
