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

    path_affiliations = os.path.join(folder_csv, "affiliations.csv")
    print(f"Loading affiliations from '{path_affiliations}'.")
    df_affiliations = pd.read_csv(path_affiliations, index_col="id_affiliation")

    path_affil_auth = os.path.join(folder_csv, "affiliation_authorships.csv")
    print(f"Loading affiliation_authorships from '{path_affil_auth}'.")
    df_affil_auth = pd.read_csv(path_affil_auth, index_col=0)

    path_areas = os.path.join(folder_csv, "areas.csv")
    print(f"Loading areas from '{path_areas}'.")
    df_areas = pd.read_csv(path_areas, index_col=0)

    path_disciplines = os.path.join(folder_csv, "disciplines.csv")
    print(f"Loading disciplines from '{path_disciplines}'.")
    df_disciplines = pd.read_csv(path_disciplines, index_col=0)

    path_facets = os.path.join(folder_csv, "facets.csv")
    print(f"Loading facets from '{path_facets}'.")
    df_facets = pd.read_csv(path_facets, index_col=0)

    path_concepts = os.path.join(folder_csv, "concepts.csv")
    print(f"Loading concepts from '{path_concepts}'.")
    df_concepts = pd.read_csv(path_concepts, index_col=0)

    path_pub_top = os.path.join(folder_csv, "publication_topics.csv")
    print(f"Loading publication_topics from '{path_pub_top}'.")
    df_pub_top = pd.read_csv(path_pub_top, index_col=0)

    return APSDataFrames(
        authors=df_authors, author_names=df_author_names,
        publications=df_publications, authorships=df_authorships,
        journals=df_journals, genders=df_genders, citations=df_citations,
        affiliations=df_affiliations, affiliation_authorships=df_affil_auth,
        areas=df_areas, disciplines=df_disciplines, facets=df_facets,
        concepts=df_concepts, publication_topics=df_pub_top)


def write_csv(folder_out: str, df_aps: APSDataFrames):
    for df, file in zip(
            [df_aps.genders, df_aps.authors, df_aps.author_names,
             df_aps.publications, df_aps.authorships, df_aps.journals, df_aps.citations,
             df_aps.affiliations, df_aps.affiliation_authorships,
             df_aps.areas, df_aps.disciplines, df_aps.facets, df_aps.concepts, df_aps.publication_topics],
            ["genders", "authors", "author_names",
             "publications", "authorships", "journals", "citations",
             "affiliations", "affiliation_authorships",
             "areas", "disciplines", "facets", "concepts", "publication_topics"]):
        path = os.path.join(folder_out, f"{file}.csv")
        print(f"Writing {file} to {path}.")
        df.to_csv(path)
