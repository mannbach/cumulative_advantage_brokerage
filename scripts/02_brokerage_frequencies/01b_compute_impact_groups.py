from cumulative_advantage_brokerage.config import parse_config
from cumulative_advantage_brokerage.constants import\
    ARG_POSTGRES_DB_APS, CS_BINS_PERCENTILES, STR_CITATIONS, STR_PRODUCTIVITY
from cumulative_advantage_brokerage.dbm import\
    PostgreSQLEngine, CumAdvBrokSession, MetricConfiguration
from cumulative_advantage_brokerage.career_series import\
    ImpactGroupsInference, CitationsBinner,\
    ProductivityBinner, StandardFilter

def main():
    config = parse_config([ARG_POSTGRES_DB_APS])

    engine = PostgreSQLEngine.from_config(config, key_dbname=ARG_POSTGRES_DB_APS)

    l_metric_ids = []
    for metric, Binner in zip(
        (STR_CITATIONS, STR_PRODUCTIVITY),
        (CitationsBinner, ProductivityBinner)):
        print(f"Working on metric '{metric}' using binner '{Binner.__name__}'")
        with CumAdvBrokSession(engine) as session:
            d_args_config = {
                "metric": metric,
                "binner": Binner.__name__,
                "binning": "quantile",
                "bins_quantiles": CS_BINS_PERCENTILES}

            d_args_config["metric"] = metric
            m_config = MetricConfiguration(
                args=d_args_config)
            print(f"Adding new configuration with args: {d_args_config}")
            session.commit_list(l=[m_config])
            l_metric_ids.append(m_config.id)

            print(f"Computing percentile borders: {CS_BINS_PERCENTILES}")
            binner = Binner(
                session=session,
                id_metric_configuration=m_config.id,
                percentiles=CS_BINS_PERCENTILES,
                collaborator_filter=StandardFilter()
            )
            binner.compute_binning_borders()
            print(f"Found borders: {binner.a_bin_values}\nInferring impact groups...")

            inf = ImpactGroupsInference(
                session=session,
                id_metric_configuration=m_config.id,
                binner=binner
            )
            inf.compute_impact_groups()
            print("Submitting results...")

    print("Done. IDs for subsequent referencing:")
    for metric, m_id in zip((STR_CITATIONS, STR_PRODUCTIVITY), l_metric_ids):
        print(f"\t'{metric}' impact groups: {m_id}")

if __name__ == "__main__":
    main()
