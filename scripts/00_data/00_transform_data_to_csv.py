from cumulative_advantage_brokerage.data import TransferToCSV
from cumulative_advantage_brokerage.config import parse_config
from cumulative_advantage_brokerage.constants import\
    ARG_TRANSF_APS_CSV_FOLDER, ARG_TRANSF_APS_CSV_FILE_DISAMB,\
    ARG_TRANSF_APS_FILE_LOG, ARG_TRANSF_APS_FOLDER_METADATA

def main():
    config = parse_config(
        list_args_required=[
            ARG_TRANSF_APS_CSV_FOLDER,
            ARG_TRANSF_APS_FOLDER_METADATA,
            ARG_TRANSF_APS_CSV_FILE_DISAMB,
            ARG_TRANSF_APS_FILE_LOG])
    transfer = TransferToCSV(
        file_disambiguation=config[ARG_TRANSF_APS_CSV_FILE_DISAMB],
        folder_metadata=config[ARG_TRANSF_APS_FOLDER_METADATA],
        folder_output=config[ARG_TRANSF_APS_CSV_FOLDER],
        file_log=config[ARG_TRANSF_APS_FILE_LOG]
    )
    transfer.integrate_data()

if __name__ == "__main__":
    main()
