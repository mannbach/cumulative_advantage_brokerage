from cumulative_advantage_brokerage.config import parse_config
from cumulative_advantage_brokerage.constants import\
    ARG_TRANSF_APS_CSV_FOLDER, ARG_TRANSF_APS_CSV_FILE_CITATIONS
from cumulative_advantage_brokerage.data import CitationDataIntegrator, read_dataframes, write_csv

def main():
    print("Loading configuration.")
    config = parse_config(
        list_args_required=[
            ARG_TRANSF_APS_CSV_FOLDER,
            ARG_TRANSF_APS_CSV_FILE_CITATIONS])
    print("Loading existing dataframes.")
    df_aps = read_dataframes(config[ARG_TRANSF_APS_CSV_FOLDER])
    integrator = CitationDataIntegrator(
        file_citation_data=config[ARG_TRANSF_APS_CSV_FILE_CITATIONS],
        df_data=df_aps
    )
    print("Integrating citations data. This might take a while.")
    df_aps = integrator.integrate_citation_data()
    print("Writing results.")
    write_csv(folder_out=config[ARG_TRANSF_APS_CSV_FOLDER], df_aps=df_aps)

if __name__ == "__main__":
    main()
