import os
from typing import Dict, Any
from argparse import ArgumentParser
from itertools import product

import pandas as pd
import numpy as np

from cumulative_advantage_brokerage.config import parse_config
from cumulative_advantage_brokerage.constants import\
    ARG_POSTGRES_DB_APS,\
    STR_CAREER_LENGTH,\
    ARG_PATH_CONTAINER_OUTPUT, N_STAGES,\
    L_MOTIF_GEN_SORTED_AGG, D_MOTIF_GEN_SORTED_AGG_MISSING
from cumulative_advantage_brokerage.career_series import\
    CollaboratorSeriesBrokerageInference
from cumulative_advantage_brokerage.dbm import\
    GENDER_FEMALE, GENDER_MALE, GENDER_UNKNOWN
from cumulative_advantage_brokerage.queries import\
    init_metric_id,\
    get_bin_values_by_id, get_auth_info, get_brokerage_events
from cumulative_advantage_brokerage.dbm import\
    PostgreSQLEngine, CumAdvBrokSession
from cumulative_advantage_brokerage.visuals import\
    plot_gender_seniority,\
    plot_author_gender_evolution,\
    plot_gen_brok_evolution


def parse_args() -> Dict[str, Any]:
    ap = ArgumentParser()
    ap.add_argument(
        "-id-cs", "--id-collaborator-series",
        default=None, type=int)
    ap.add_argument(
        "--normalize",
        default=False, action="store_true")


    d_a = vars(ap.parse_args())

    return d_a

def get_auth_evolution(d_auth_info: pd.DataFrame) -> pd.Series:
    d_auth_info = d_auth_info.query("birth != death").copy()
    d_auth_info["birth_y"] = d_auth_info["birth"].dt.year
    d_auth_info["death_y"] = d_auth_info["death"].dt.year

    s_birth_gen_cnt = d_auth_info.groupby(["gender", "birth_y"])["birth"].count()
    s_death_gen_cnt = d_auth_info.groupby(["gender", "death_y"])["death"].count()

    df_birth_cnt = s_birth_gen_cnt.reset_index()
    df_death_cnt = s_death_gen_cnt.reset_index()

    # Rename the year columns to a common name 'year'
    df_birth_cnt = df_birth_cnt.rename(columns={'birth_y': 'year'})
    df_death_cnt = df_death_cnt.rename(columns={'death_y': 'year'})

    # Merge the two dataframes on 'gender' and 'y' using an outer join
    df_birth_death_cnt = pd.merge(
        df_birth_cnt, df_death_cnt,
        on=['gender', 'year'], how='outer').fillna(0)

    # Set the new index to be 'gender' and 'year'
    gs_birth_death_cnt = df_birth_death_cnt\
        .set_index(['gender', 'year'])\
        .sort_index(level=[0,1])\
        .groupby("gender")\
        .cumsum()\
        .astype(int)
    return (gs_birth_death_cnt["birth"] - gs_birth_death_cnt["death"])

def merge_brokerage_events_authors(
        d_brokage_events: pd.DataFrame,
        d_author_info: pd.DataFrame) -> pd.DataFrame:
    d_brok_coll = d_brokage_events
    for key_idc in "abc":
        d_brok_coll = pd.merge(
            d_brok_coll,
            d_author_info["birth"],
            left_on=f"id_collaborator_{key_idc}",
            right_index=True,
            suffixes=(None, f"_{key_idc}"))
    d_brok_coll = d_brok_coll.rename(columns={"birth": "birth_a"})

    for role in "abc":
        d_brok_coll[f"age_{role}"] =\
            (d_brok_coll["t_ac"] - d_brok_coll[f"birth_{role}"]).dt.days / 365
        d_brok_coll =\
            d_brok_coll[(d_brok_coll[f"gender_{role}"] != GENDER_UNKNOWN.gender)]

    return d_brok_coll

def main():
    config = parse_config([ARG_POSTGRES_DB_APS])
    engine = PostgreSQLEngine.from_config(config, key_dbname=ARG_POSTGRES_DB_APS)
    args = parse_args()

    file_out_seniority = os.path.join(
        config[ARG_PATH_CONTAINER_OUTPUT], "05a_gender_seniority.pdf")

    file_out_evolution = os.path.join(
        config[ARG_PATH_CONTAINER_OUTPUT], "05b_author_evolutions.pdf")

    with CumAdvBrokSession(engine) as session:
        print("Getting career series bins...")
        id_metric_series = args["id_collaborator_series"]
        if id_metric_series is None:
            id_metric_series = init_metric_id(
                metric_args=dict(
                    metric=STR_CAREER_LENGTH,
                    type=CollaboratorSeriesBrokerageInference.__name__),
                session=session)
        a_bins_career_length = get_bin_values_by_id(
            session, id_metric_series)
        print(f"Got bins {a_bins_career_length}.")

        print("Getting aggregated author info...")
        d_author_info = get_auth_info(session, filtered=False)
        print(f"Got {len(d_author_info)} results.\n")

        print("Getting evolution of authors per gender...")
        s_author_evolution = get_auth_evolution(d_author_info)
        print("First publication by gender:")
        for gender in (GENDER_FEMALE, GENDER_MALE, GENDER_UNKNOWN):
            print(
                f"\t{gender.gender}: ",
                s_author_evolution[gender.gender]\
                    .where(s_author_evolution[gender.gender] > 0)\
                    .sort_index()\
                    .index[0])

        print("\nGetting brokerage events...")
        d_brokage_events = get_brokerage_events(session=session, join_projects=True)
        d_brokage_events["t_ac_y"] = d_brokage_events["t_ac"].dt.year
        gs_gabc_y_cnt=d_brokage_events\
            .groupby(["gender_a", "gender_b", "gender_c", "t_ac_y"])\
            .size()
        print((f"Got {len(d_brokage_events)} events.\n"
               "First event per gender:"))
        for t_gender in L_MOTIF_GEN_SORTED_AGG:
            print("\t", t_gender, gs_gabc_y_cnt[t_gender].index.min())
            for t_missing in D_MOTIF_GEN_SORTED_AGG_MISSING[t_gender]:
                print("\t", t_missing, gs_gabc_y_cnt[t_missing].index.min())

    print("\nMerging brokerage events with author info and filtering by gender...")
    d_brokerage_auth = merge_brokerage_events_authors(
        d_brokage_events, d_author_info)
    print((f"This removed {len(d_brokage_events) - len(d_brokerage_auth)}\
            / {len(d_brokage_events)} events.\n"))

    print("Computing histograms by gender of a and c...")
    a_histograms_b = np.zeros((4, 2, N_STAGES-1))
    a_hist_joint_ac = np.zeros((4, N_STAGES-1, N_STAGES-1))
    gs_gacb_age = d_brokerage_auth\
        .groupby(["gender_a", "gender_c", "gender_b"])\
        [["age_b"]]
    gs_gac_age = d_brokerage_auth\
        .groupby(["gender_a", "gender_c"])[["age_a", "age_c"]]
    for i, (gender_a, gender_c) in enumerate(product((GENDER_FEMALE, GENDER_MALE), repeat=2)):
        print("\t", gender_a.gender, gender_c.gender)
        for j, gender_b in enumerate(((GENDER_FEMALE, GENDER_MALE))):
            print("\t\tGender broker ", gender_b.gender)
            d_gac_age = gs_gacb_age\
                .get_group((gender_a.gender, gender_c.gender, gender_b.gender))
            # d_gac_age = d_gac_age[d_gac_age["age_b"] <= a_bins_lng[-1]] # Filter out last stage
            a_histograms_b[i,j] = np.histogram(
                d_gac_age["age_b"],
                bins=a_bins_career_length,
                density=True)[0]

        d_gac_age = gs_gac_age.get_group((gender_a.gender, gender_c.gender))
        a_hist_joint_ac[i] = np.histogram2d(
            d_gac_age["age_a"],
            d_gac_age["age_c"],
            bins=a_bins_career_length,
            density=True)[0]

    print("Computing overall joint distribution")
    a_hist_joint = np.histogram2d(
        d_brokerage_auth["age_a"],
        d_brokerage_auth["age_c"],
        bins=a_bins_career_length, density=True)[0]

    fig_sen = plot_gender_seniority(
        gs_gabc_y_cnt=gs_gabc_y_cnt,
        s_gacb_cnt=d_brokerage_auth\
            .groupby(["gender_a", "gender_c"])["gender_b"]\
            .value_counts(),
        a_histograms_b=a_histograms_b,
        a_h_cmp_joint=a_hist_joint_ac / a_hist_joint,
        a_hist_joint=a_hist_joint,
        a_histograms=a_hist_joint_ac,
    )
    print(f"Saving gender seniority disparities to {file_out_seniority}.")
    fig_sen.savefig(file_out_seniority, bbox_inches="tight")

    print(f"Saving author evolution to {file_out_evolution}.")
    fig_evo = plot_author_gender_evolution(
        s_auth_active_gen=s_author_evolution
    )
    fig_evo.savefig(file_out_evolution, bbox_inches="tight")

    if args["normalize"]:
        fig_sen_norm, _ = plot_gen_brok_evolution(
            gs_gabc_y_cnt=gs_gabc_y_cnt,
            s_auth_active_gender=s_author_evolution,
            normalize=True,
            aggregate_mixed=True,
            plot_init_cnt=False,
            plot_init=False)
        fig_sen_norm.legend(
            frameon=False, ncol=4, loc="upper center")
        file_out_seniority_norm = os.path.join(
            config[ARG_PATH_CONTAINER_OUTPUT], "si_gender_brokerage_count_norm.pdf")
        print(f"Saving normalized gender seniority disparities to {file_out_seniority_norm}.")
        fig_sen_norm.savefig(file_out_seniority_norm, bbox_inches="tight")


if __name__ == "__main__":
    main()
