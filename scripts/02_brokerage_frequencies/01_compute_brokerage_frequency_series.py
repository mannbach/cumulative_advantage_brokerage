from typing import List, Any
from sqlalchemy import select, func, and_

import numpy as np

from cumulative_advantage_brokerage.config import parse_config
from cumulative_advantage_brokerage.constants import ARG_POSTGRES_DB_APS
from cumulative_advantage_brokerage.dbm import PostgreSQLEngine, CumAdvBrokSession
from cumulative_advantage_brokerage.career_series import\
    CollaboratorSeriesLongevityBinner, CollaboratorSeriesCitationBinner,\
    CollaboratorSeriesProductivityBinner

BINS_QUANTILES = [0.0, 0.5, 0.7, 0.85, 0.95, 1.0]
# BINS_QUANTILES = [0., .6, .78, .86, .91, 1.]

def commit_list(session: APSSession, l: List[Any]):
    session.add_all(l)
    session.commit()
    for el in l:
        session.refresh(el)

def main():
    config = parse_config([ARG_POSTGRES_DB_APS])

    engine = PostgreSQLEngine.from_config(config, key_dbname=ARG_POSTGRES_DB_APS)

    with APSSession(engine) as session:
        d_args_config = {
            "binning": "quantile",
            "bins_quantiles": BINS_QUANTILES}

        for metric, Binner in zip(
                ("longevity", "citations", "productivity"),
                (   CollaboratorSeriesLongevityBinner,
                    CollaboratorSeriesCitationBinner,
                    CollaboratorSeriesProductivityBinner)):
            print(f"Working on metric '{metric}'.")

            d_args_config["metric"] = metric
            m_config = session.class_map.MetricConfiguration(
                args=d_args_config)
            print(f"Adding new configuration with args: {d_args_config}")
            commit_list(session=session, l=[m_config])

            binner = Binner.from_percentiles(
                id_metric_configuration=m_config.id,
                percentiles=BINS_QUANTILES,
                session=session
            )
            l_hist = []
            print("Submitting results...")
            for result in binner.generate_binning():
                l_hist += result.l_series
                if len(l_hist) >= 100000:
                    commit_list(session=session, l=l_hist)
                    l_hist = []
            commit_list(session=session, l=l_hist)

    print("Done.")

if __name__ == "__main__":
    main()
