from typing import Dict, Any
from argparse import ArgumentParser
from itertools import product
import warnings

from cumulative_advantage_brokerage.config import parse_config
from cumulative_advantage_brokerage.constants import\
    ARG_POSTGRES_DB_APS, TPL_STR_IMPACT,\
    STR_CITATIONS, STR_PRODUCTIVITY, STR_CAREER_LENGTH,\
    N_RESAMPLES_DEFAULT, STR_BF_CMP, STR_BR_CMP, STR_BR_COR
from cumulative_advantage_brokerage.stats import\
    CollaboratorSeriesBrokerageComparison,\
    CollaboratorSeriesRateStageComparison,\
    CollaboratorSeriesRateStageCorrelation,\
    GrouperDummy,\
    GrouperGender, GrouperRole, GrouperBirthDecade,\
    MannWhitneyPermutTest, KolmogorovSmirnovPermutTest,\
    ContKolmogorovSmirnovPermutTest,\
    SpearmanPermutTest, PearsonPermutTest
from cumulative_advantage_brokerage.dbm import\
    PostgreSQLEngine, CumAdvBrokSession, MetricConfiguration,\
    get_single_result, select_latest_metric_config_id_by_metric,\
    get_bin_values_by_id

GROUPERS = {g.name: g\
    for g in (GrouperDummy, GrouperGender, GrouperRole, GrouperBirthDecade)}
TESTS = {t.label_file: t\
    for t in (MannWhitneyPermutTest, KolmogorovSmirnovPermutTest, ContKolmogorovSmirnovPermutTest, MannWhitneyPermutTest, SpearmanPermutTest, PearsonPermutTest)}
CMP_OPTIONS_TESTS = {
    STR_BF_CMP: (MannWhitneyPermutTest, KolmogorovSmirnovPermutTest),
    STR_BR_CMP: (MannWhitneyPermutTest, ContKolmogorovSmirnovPermutTest),
    STR_BR_COR: (SpearmanPermutTest, PearsonPermutTest),
}
CMP_OPTIONS_CLS = {
    STR_BF_CMP: CollaboratorSeriesBrokerageComparison,
    STR_BR_CMP: CollaboratorSeriesRateStageComparison,
    STR_BR_COR: CollaboratorSeriesRateStageCorrelation,
}

def parse_args() -> Dict[str, Any]:
    ap = ArgumentParser()
    ap.add_argument("-c", "--comparisons",
                    choices=list(CMP_OPTIONS_TESTS.keys()),
                    default=list(CMP_OPTIONS_TESTS.keys()),
                    type=str,
                    nargs="+")
    ap.add_argument(
        "-idcs-cs", "--id-collaborator-series",
        default=None, type=int)
    ap.add_argument("-idig-cit", f"--id-impact-group-{STR_CITATIONS}",
        default=None, type=int)
    ap.add_argument("-idig-prd", f"--id-impact-group-{STR_PRODUCTIVITY}",
        default=None, type=int)

    ap.add_argument("-r", "--n-resamples",
                    type=int, default=N_RESAMPLES_DEFAULT)
    ap.add_argument("-g", "--groupers",
                    choices=list(GROUPERS.keys()),
                    default=list(GROUPERS.keys()),
                    type=str,
                    nargs="+")
    ap.add_argument("-t", "--tests",
                    choices=list(TESTS.keys()),
                    default=list(TESTS.keys()),
                    type=str,
                    nargs="+")

    d_a = vars(ap.parse_args())

    return d_a

def main():
    config = parse_config([ARG_POSTGRES_DB_APS])
    engine = PostgreSQLEngine.from_config(config, key_dbname=ARG_POSTGRES_DB_APS)
    args = parse_args()

    with CumAdvBrokSession(engine) as session:
        id_metric_career = args["id_collaborator_series"]
        if id_metric_career is None:
            id_metric_career = get_single_result(
                session=session,
                query=select_latest_metric_config_id_by_metric(STR_CAREER_LENGTH))
            assert id_metric_career is not None, "No career series ID found."
            warnings.warn(
                ("No career series ID provided. "
                 "Using latest career series. "
                 "This only works if the last computation of "
                 "brokerage frequencies was successful!\n"
                 f"Found ID '{id_metric_career}'."))

        a_bins_career = get_bin_values_by_id(session, id_metric_career)

        for comparison, name_test, name_grouper in product(args["comparisons"], args["tests"], args["groupers"]):
            test = TESTS[name_test](**args)
            grouper = GROUPERS[name_grouper]
            if test.__class__ not in CMP_OPTIONS_TESTS[comparison]:
                print(f"Skipping test `{name_test}` for comparison `{comparison}`.")
                continue

            print(f"Working on comparison=`{comparison}`, test=`{test.label_file}` and grouper=`{grouper.name}`.")
            l_metric_ids = []
            for metric_impact in TPL_STR_IMPACT:
                print("\t",metric_impact)
                id_impact_group = args[f"id_impact_group_{metric_impact}"]
                if id_impact_group is None:
                    id_impact_group = get_single_result(
                        session=session,
                        query=select_latest_metric_config_id_by_metric(metric_impact))
                    warnings.warn(
                        (f"No impact group ID provided for `{metric_impact}`. "
                        "Using latest impact group. "
                        "This only works if the last computation of "
                        "the impact group was successful!\n"
                        f"Found ID '{id_impact_group}'."))
                    assert id_impact_group is not None, "No impact group ID found."
                m_config_cmp = MetricConfiguration(
                    args={
                        "type": CMP_OPTIONS_CLS[comparison].__name__,
                        "comparison": comparison,
                        "id_metric_config_career": id_metric_career,
                        "id_metric_config_impact": id_impact_group,
                        "metric_success": metric_impact,
                        "stat_test": test.label_file,
                        "grouper": grouper.name if grouper is not None else None,
                        "n_resample_permut": test.n_resamples,
                        "n_resample_bootstrap": args["n_resamples"],
                    })
                session.commit_list(l=[m_config_cmp])
                print(f"\t\tStoring results under configuration ID `{m_config_cmp.id}`.")
                l_metric_ids.append(m_config_cmp.id)

                cmp = CMP_OPTIONS_CLS[comparison](
                    session=session,
                    id_metric_config_comparison=m_config_cmp.id,
                    id_metric_config_career=id_metric_career,
                    id_metric_config_impact_group=id_impact_group,
                    statistical_test=test,
                    grouper=grouper)\
                        if comparison == STR_BF_CMP else\
                    CMP_OPTIONS_CLS[comparison](
                    session=session,
                    id_metric_config_comparison=m_config_cmp.id,
                    id_metric_config_career=id_metric_career,
                    id_metric_config_impact_group=id_impact_group,
                    statistical_test=test,
                    bins=a_bins_career,
                    grouper=grouper)

                cmp.init_cached_data()

                if len(grouper.possible_values) == 0:
                    l_res = list(cmp.generate_comparisons())
                    session.commit_list(l_res)
                else:
                    for g_val in grouper.possible_values:
                        print(f"Grouping on {grouper.name}={g_val}")
                        l_res = list(
                            cmp.generate_comparisons(
                                grouping_key=g_val))
                        print(f"\t\tCommitting {len(l_res)} results to DB.")
                        session.commit_list(l_res)

            print("Done. IDs for subsequent referencing:")
            for metric, m_id in zip((STR_CITATIONS, STR_PRODUCTIVITY), l_metric_ids):
                print(f"\t'{metric}' impact groups: {m_id}")

if __name__ == "__main__":
    main()
