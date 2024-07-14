import os
from typing import Dict, Any
from argparse import ArgumentParser
import warnings

import numpy as np

from cumulative_advantage_brokerage.config import parse_config
from cumulative_advantage_brokerage.constants import\
    ARG_POSTGRES_DB_APS, TPL_STR_IMPACT,\
    STR_CITATIONS, STR_PRODUCTIVITY, STR_CAREER_LENGTH,\
    ARG_PATH_CONTAINER_OUTPUT
from cumulative_advantage_brokerage.career_series import\
    CitationsBinner, ProductivityBinner, CareerLengthBinner,\
    StandardFilter
from cumulative_advantage_brokerage.dbm import\
    get_single_result, select_latest_metric_config_id_by_metric,\
    get_bin_values_by_id, PostgreSQLEngine, CumAdvBrokSession
from cumulative_advantage_brokerage.visuals import plot_heterogeneity

def parse_args() -> Dict[str, Any]:
    ap = ArgumentParser()
    ap.add_argument(
        "-idcs-cs", "--id-collaborator-series",
        default=None, type=int)
    ap.add_argument("-idig-cit", f"--id-impact-group-{STR_CITATIONS}",
        default=None, type=int)
    ap.add_argument("-idig-prd", f"--id-impact-group-{STR_PRODUCTIVITY}",
        default=None, type=int)

    d_a = vars(ap.parse_args())

    return d_a

def init_metric_id(metric: str, session) -> int:
    id_metric = get_single_result(
        session=session,
        query=select_latest_metric_config_id_by_metric(metric))
    assert id_metric is not None, "No career series ID found."
    warnings.warn(
        (f"No metric ID provided for {metric}. "
        "Using latest metric. "
        "This only works if the last computation of "
        "the respective binning was successful!\n"
        f"Found ID '{id_metric}'."))
    return id_metric

def main():
    config = parse_config([ARG_POSTGRES_DB_APS])
    engine = PostgreSQLEngine.from_config(config, key_dbname=ARG_POSTGRES_DB_APS)
    args = parse_args()

    file_out = os.path.join(config[ARG_PATH_CONTAINER_OUTPUT], "02_heterogeneity.pdf")

    l_l_vals = []
    l_bins_stages = []
    with CumAdvBrokSession(engine) as session:
        print("Collecting bins...")
        for key_metric, str_metric, Binner in zip(
                ("id_collaborator_series", f"id_impact_group_{STR_CITATIONS}", f"id_impact_group_{STR_PRODUCTIVITY}"),
                (STR_CAREER_LENGTH, *TPL_STR_IMPACT),
                (CareerLengthBinner, CitationsBinner, ProductivityBinner)):
            print(f"Working on `{str_metric}`...\nGetting bins.")
            id_metric = args[key_metric]
            if id_metric is None:
                id_metric = init_metric_id(str_metric, session)
            l_bins_stages.append(get_bin_values_by_id(session, id_metric))
            print(f"\tBins: {l_bins_stages[-1]}")

            print("Getting max values...")
            binner = Binner(
                session=session,
                id_metric_configuration=id_metric,
                percentiles=None,
                collaborator_filter=StandardFilter())
            l_l_vals.append(np.asarray([max_val\
                for _, max_val in session.execute(binner.create_query_max_value())]))
            print(f"\tLength max values: {len(l_l_vals[-1])}")

    fig = plot_heterogeneity(
        tpl_l_vals=l_l_vals,
        tpl_l_bins_stages=l_bins_stages)
    print(f"Saving figure to {file_out}...")
    fig.savefig(file_out, bbox_inches='tight')

if __name__ == "__main__":
    main()
