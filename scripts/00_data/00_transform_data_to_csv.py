from cumulative_advantage_brokerage.data import TransferToCSV
from cumulative_advantage_brokerage.config import parse_config
from cumulative_advantage_brokerage.constants import\
    ARG_FOLDER_DATA, ARG_CSV_INT_FILE_DISAMB,\
    ARG_CSV_INT_FILE_LOG, ARG_CSV_INT_FOLDER_METADATA,\
    ARG_CSV_INT_N_PUBLICATIONS

def main():
    config = parse_config(
        list_args_required=[
            ARG_FOLDER_DATA,
            ARG_CSV_INT_FOLDER_METADATA,
            ARG_CSV_INT_FILE_DISAMB,
            ARG_CSV_INT_FILE_LOG],
        list_args_optional=[
            ARG_CSV_INT_N_PUBLICATIONS])
    transfer = TransferToCSV(**config)
    transfer.integrate_data(n_projects=config[ARG_CSV_INT_N_PUBLICATIONS])

if __name__ == "__main__":
    main()
