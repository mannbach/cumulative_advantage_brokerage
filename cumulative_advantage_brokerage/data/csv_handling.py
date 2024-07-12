import os

import pandas as pd

from .data_classes import APSDataFrames

def read_dataframes(folder_csv: str) -> APSDataFrames:
    path_genders = os.path.join(folder_csv, "genders.csv")
    print(f"Loading genders from '{path_genders}'.")
    df_genders = pd.read_csv(path_genders, index_col="id_gender")

    path_authors = os.path.join(folder_csv, "authors.csv")
    print(f"Loading authors from '{path_authors}'.")
    df_authors = pd.read_csv(path_authors, index_col="id_author")

    path_author_names = os.path.join(folder_csv, "author_names.csv")
    print(f"Loading author_names from '{path_author_names}'.")
    df_author_names = pd.read_csv(path_author_names, index_col=0)

    path_publications = os.path.join(folder_csv, "publications.csv")
    print(f"Loading publications from '{path_publications}'.")
    df_publications = pd.read_csv(path_publications, index_col="id_publication", parse_dates=["timestamp"])

    path_authorships = os.path.join(folder_csv, "authorships.csv")
    print(f"Loading authorships from '{path_authorships}'.")
    df_authorships = pd.read_csv(path_authorships, index_col=0)

    path_journals = os.path.join(folder_csv, "journals.csv")
    print(f"Loading journals from '{path_journals}'.")
    df_journals = pd.read_csv(path_journals, index_col="id_journal")

    path_citations = os.path.join(folder_csv, "citations.csv")
    print(f"Loading citations from '{path_citations}'.")
    df_citations = None
    try:
        df_citations = pd.read_csv(path_citations, index_col=0)
    except FileNotFoundError:
        print(f"Could not find '{path_citations}'. Continuing without.")

    return APSDataFrames(
        authors=df_authors, author_names=df_author_names,
        publications=df_publications, authorships=df_authorships,
        journals=df_journals, genders=df_genders, citations=df_citations)

def write_csv(folder_out: str, df_aps: APSDataFrames):
    for df, file in zip(
            [df_aps.genders, df_aps.authors, df_aps.author_names,
             df_aps.publications, df_aps.authorships, df_aps.journals, df_aps.citations],
            ["genders", "authors", "author_names",
             "publications", "authorships", "journals", "citations"]):
        path = os.path.join(folder_out, f"{file}.csv")
        print(f"Writing {file} to {path}.")
        df.to_csv(path)
