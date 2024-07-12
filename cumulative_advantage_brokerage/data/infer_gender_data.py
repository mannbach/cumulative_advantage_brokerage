import os

import nomquamgender as nqg
import pandas as pd
import numpy as np

from ..constants import MAP_GENDERNQ_ID
from .data_classes import APSDataFrames

class GenderInference:
    def __init__(self, threshold: float):
        self.threshold = threshold

    @staticmethod
    def aggregate_maj_gender_nq(s_names: pd.Series) -> int:
        n_gm = (s_names == "gm").sum()
        n_gf = (s_names == "gf").sum()

        if n_gm == n_gf:
            return MAP_GENDERNQ_ID["-"]
        elif n_gm > n_gf:
            return MAP_GENDERNQ_ID["gm"]
        return MAP_GENDERNQ_ID["gf"]

    def infer_gender(self, dfs: APSDataFrames) -> APSDataFrames:
        model = nqg.NBGC()

        if self.threshold == -1:
            print(("Tuning model threshold.\n"
                   "\tFiltering out initials-only-names."))
            mask_names_initial = dfs.author_names["name"].str.contains("^\w\.?\s+")
            print(f"\tFound {mask_names_initial.sum()} / {len(mask_names_initial)} initial-only-names.")
            model.tune(
                names=dfs.author_names.loc[~mask_names_initial, "name"],
                candidates=np.linspace(0.49, 0.02, 20).round(2))
            print(f"\tIdentified threshold '{model.threshold}'.")
            self.threshold = model.threshold
        elif isinstance(self.threshold, float):
            model.threshold = self.threshold

        print(f"Inferring gender with threshold '{model.threshold}'.")
        dfs.author_names["gender_nq"] = model.classify(dfs.author_names["name"])

        print("Aggregating based on majority-vote.")
        dfs.authors["id_gender_nq"] = dfs.author_names.groupby("id_author")["gender_nq"].aggregate(GenderInference.aggregate_maj_gender_nq)

        print(f"Inferred gender (by majority-vote-aggregation) for {(dfs.authors['id_gender_nq'] != MAP_GENDERNQ_ID['-']).sum()}/{len(dfs.authors)} authors.")

        return dfs


def main():
    config = parse_config(
        list_args_required=[ARG_FOLDER_DATA],
        list_args_optional=[ARG_NOMQUAM_THRESHOLD])

    dfs = read_dataframes(folder_csv=config[ARG_FOLDER_DATA])

    path_author_names = os.path.join(config[ARG_FOLDER_DATA], "author_names.csv")
    print(f"Writing author_names to '{path_author_names}'.")
    dfs.author_names.to_csv(path_author_names)

    path_authors = os.path.join(config[ARG_FOLDER_DATA], "authors.csv")
    print(f"Writing authors to '{path_authors}'.")
    dfs.authors.to_csv(path_authors)

    print(f"Inferring gender with threshold '{model.threshold}'.")
    dfs.author_names["gender_nq"] = model.classify(dfs.author_names["name"])



if __name__ == "__main__":
    main()
