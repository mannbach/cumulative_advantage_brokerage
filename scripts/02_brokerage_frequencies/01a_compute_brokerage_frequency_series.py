from cumulative_advantage_brokerage.config import parse_config
from cumulative_advantage_brokerage.constants import\
    ARG_POSTGRES_DB_APS, CS_BINS_PERCENTILES, STR_CAREER_LENGTH
from cumulative_advantage_brokerage.dbm import\
    PostgreSQLEngine, CumAdvBrokSession, MetricConfiguration
from cumulative_advantage_brokerage.career_series import CollaboratorSeriesBrokerageInference, CareerLengthBinner, StandardFilter

def main():
    config = parse_config([ARG_POSTGRES_DB_APS])

    engine = PostgreSQLEngine.from_config(config, key_dbname=ARG_POSTGRES_DB_APS)

    with CumAdvBrokSession(engine) as session:
        d_args_config = {
            "binning": "quantile",
            "bins_quantiles": CS_BINS_PERCENTILES}

        d_args_config["metric"] = STR_CAREER_LENGTH
        m_config = MetricConfiguration(
            args=d_args_config)
        print(f"Adding new configuration with args: {d_args_config}")
        session.commit_list(l=[m_config])

        print(f"Computing percentile borders: {CS_BINS_PERCENTILES}")
        binner = CareerLengthBinner(
            session=session,
            id_metric_configuration=m_config.id,
            percentiles=CS_BINS_PERCENTILES,
            collaborator_filter=StandardFilter()
        )
        binner.compute_binning_borders()
        print(f"Found borders: {binner.a_bin_values}\nComputing series...")

        cs = CollaboratorSeriesBrokerageInference(
            session=session,
            id_metric_configuration=m_config.id,
            binner=binner,
            collaborator_filter=StandardFilter(),
        )
        l_hist = []
        print("Submitting results...")
        for result in cs.generate_series():
            l_hist += result.l_series
            if len(l_hist) >= 100000:
                session.commit_list(l=l_hist)
                l_hist = []
        session.commit_list(l=l_hist)

    print(f"Done.\nCareer series metric ID: {m_config.id}")

if __name__ == "__main__":
    main()
