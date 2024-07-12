from argparse import ArgumentParser
from cumulative_advantage_brokerage.data import GenderInference, read_dataframes, write_csv
from cumulative_advantage_brokerage.config import parse_config
from cumulative_advantage_brokerage.constants import ARG_TRANSF_APS_CSV_FOLDER, ARG_NOMQUAM_THRESHOLD

def parse_cli_args():
    args = ArgumentParser()
    args.add_argument("--threshold", type=float, default=-1, dest=ARG_NOMQUAM_THRESHOLD)
    return args.parse_args()

def main():
    config = parse_config(
        list_args_required=[ARG_TRANSF_APS_CSV_FOLDER],
        list_args_optional=[ARG_NOMQUAM_THRESHOLD])
    config.update(parse_cli_args().__dict__)

    dfs = read_dataframes(folder_csv=config[ARG_TRANSF_APS_CSV_FOLDER])

    inf = GenderInference(threshold=config[ARG_NOMQUAM_THRESHOLD])

    dfs = inf.infer_gender(dfs)

    write_csv(folder_out=config[ARG_TRANSF_APS_CSV_FOLDER], dfs=dfs)

if __name__ == "__main__":
    main()
