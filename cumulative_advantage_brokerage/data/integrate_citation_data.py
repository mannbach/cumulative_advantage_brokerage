import pandas as pd

from .data_classes import APSDataFrames

class CitationDataIntegrator:
    df_data: APSDataFrames
    file_citation_data: str
    folder_aps_data: str

    def __init__(
            self,
            file_citation_data: str,
            df_data: APSDataFrames) -> None:
        self.df_data = df_data
        self.file_citation_data = file_citation_data

    def integrate_citation_data(self) -> APSDataFrames:
        df_cit = pd.read_csv(self.file_citation_data)

        self.df_data.publications["id"] = self.df_data.publications.index
        df_cit = df_cit.merge(
                self.df_data.publications,
                left_on="citing_doi",
                right_on="doi",
                suffixes=("", "_publication_citing"))\
            .merge(
                self.df_data.publications,
                left_on="cited_doi",
                right_on="doi",
                suffixes=("", "_publication_cited"))\
            .rename(
                columns={"id": "id_publication_citing"}
            )
        self.df_data.publications = self.df_data.publications.drop(columns="id")
        self.df_data.citations = df_cit[
            ["id_publication_citing", "id_publication_cited"]]
        return self.df_data
