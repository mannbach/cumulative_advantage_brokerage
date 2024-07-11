"""APS integration.
"""
from collections import defaultdict
from itertools import combinations
from datetime import datetime
import json
import os
from csv import DictReader
from typing import Any, Dict, List, Set, Tuple, Union

import pandas as pd
from unidecode import unidecode

from .data_classes import\
    Author, AuthorName, Publication,\
    Authorship, Journal, APSCollection
from ..constants.constants import\
    ID_GENDER_UNKNOWN, ID_GENDER_FEMALE,\
    ID_GENDER_MALE, MAP_GENDER_ID

class TransferToCSV():
    """Performs the integration of the APS dataset.
    See `APSIntegrator.integrate_data` for details.
    """
    collection: APSCollection

    _folder_metadata: str
    _file_disambiguation: str
    _folder_output: str
    _path_log: str

    _id_author: int
    _id_author_name: int
    _id_publication: int
    _id_journal: int
    _id_affiliation: int
    _id_area: int
    _id_discipline: int
    _id_concept: int
    _id_facet: int

    _cache_affiliations: Dict[str, int]

    _cache_areas: Dict[str, int]
    _cache_disciplines: Dict[str, int]
    _cache_concepts: Dict[str, int]
    _cache_facets: Dict[str, int]

    DicDisambCollaborator = Dict[int, List[Dict[str, Union[str, int]]]]

    def __init__(
            self,
            folder_metadata: str,
            file_disambiguation: str,
            folder_output: str,
            file_log: Union[str, None] = None) -> None:
        self._path_log = file_log
        self._folder_metadata = folder_metadata
        self._folder_output = folder_output
        self._file_disambiguation = file_disambiguation

        self._id_author = 0
        self._id_author_name = 0
        self._id_publication = 0
        self._id_journal = 0
        self._id_authorship = 0
        self._id_affiliation = 0
        self._id_area = 0
        self._id_discipline = 0
        self._id_concept = 0
        self._id_facet = 0

        self._cache_affiliations = {}
        self._cache_areas = {}
        self._cache_disciplines = {}
        self._cache_concepts = {}
        self._cache_facets = {}

        self.collection = APSCollection()

    def _integrate_project_from_json(self, dic_from_json: Dict[str, Any], id_journal: int)\
            -> Publication:
        publication = Publication(
            id_publication=self._id_publication,
            timestamp=datetime.fromisoformat(dic_from_json["date"]),
            doi=dic_from_json["identifiers"]["doi"],
            id_journal=id_journal
        )
        self._id_publication += 1
        self.collection.publications.append(publication)
        return publication

    @staticmethod
    def _clean_collaborator_name(name: str) -> str:
        return name.replace("\u2009", " ")

    @staticmethod
    def _get_first_name_key(name: str) -> str:
        """Processes a given name string to return a first name key.
        This includes (in this order):
        1. replacing "-" with a " "
        2. retrieving first name at first position when splitting at " "
        3. transform to lower case
        4. transform to ASCII characters (e.g., 'é' to 'e')

        Args:
            name (str): The name to be transformed

        Returns:
            str: The transformed name
        """
        return unidecode(name.replace("-", "").split(" ")[0].lower())

    @staticmethod
    def _first_name_is_initial_of_other(initial: str, name: str) -> bool:
        """Tests whether one string is a prefix or initial of another.

        Args:
            initial (str): The prefix or initial candidate.
            name (str): The full name string in which `initial` is supposedly nested.

        Returns:
            bool: Whether `initial` is a prefix or initial of `name`.
        """
        initial_proc = initial[:-1] if initial[-1] == '.' else initial
        return name.startswith(initial_proc)

    @staticmethod
    def _merge_names(dic_map_name_dois: Dict[str, Tuple[str, str]]):
        """Tries to merge names using an agglomerative approach.
        To this end, the following algorithm is executed:
        1. for each first name key n
        2.     for each other first name key m
        3.         add m to candidate set C_n if n is a prefix of m
        4. for each first name key n and its candidate set C_n
        5.     merge n with its merge candidate if it is unique (|C_n| = 1)
        5. repeat at 1.

        Args:
            dic_map_name_dois (Dict[str, Tuple[str, str]]): All names previously assigned to one author. Each key is used as a first name key and might be merged with those it is a unique prefix of. This dictionary will be changed in place.
        """
        merged = True # keep track of convergence
        while merged:
            merged = False

            # collect merge candidates in dictionary
            d_map_candidates: Dict[str, Set[str]] = defaultdict(set)

            # go through all possible combinations
            for fn_k0, fn_k1 in combinations(dic_map_name_dois.keys(), 2):
                # test if one first name key can be the prefix of the other
                if TransferToCSV._first_name_is_initial_of_other(fn_k0, fn_k1):
                    d_map_candidates[fn_k0].add(fn_k1) # add to candidate set
                elif TransferToCSV._first_name_is_initial_of_other(fn_k1, fn_k0):
                    d_map_candidates[fn_k1].add(fn_k0)

            # iterate over all candidate sets
            for fn_k0, candidates in d_map_candidates.items():
                if len(candidates) > 1: # ignore if ambiguous
                    continue

                # merge first name key into its parent
                (fn_k1,) = candidates
                for name_merge, doi_merge in dic_map_name_dois[fn_k0]:
                    dic_map_name_dois[fn_k1].add((name_merge, doi_merge))
                del dic_map_name_dois[fn_k0]
                merged = True # do another round

    def _integrate_collaborators_merged(self,
            dic_map_name_dois: Dict[str, Tuple[str, str]],
            gender_curr: str,
            dic_map_doi_collaborator: Dict[Tuple[str, str], AuthorName]):
        # For each merged name, we create a new author
        for i, (_, s_names) in enumerate(dic_map_name_dois.items()):
            self.collection.authors.append(
                Author(
                    id_author=self._id_author,
                    id_gender=MAP_GENDER_ID[gender_curr]\
                        if gender_curr != "" else MAP_GENDER_ID["unknown"],
                    disambiguated=True
                )
            )

            d_name_int_cache = {} # Only add unique names once
            for (name_int, doi_int) in s_names:
                if name_int not in d_name_int_cache:
                    auth_name = AuthorName(
                            id_author_name=self._id_author_name,
                            id_author=self._id_author,
                            name=name_int)
                    self.collection.author_names.append(auth_name)
                    d_name_int_cache[name_int] = auth_name
                    self._id_author_name += 1
                # Store all names and DOIs in map (for downstream assignmnet)
                dic_map_doi_collaborator[(doi_int, name_int)] = d_name_int_cache[name_int]

            self._id_author += 1

    def _integrate_collaborators_from_disamb(self) -> Dict[Tuple[str, str], AuthorName]:
        # Maps collaborator id to tuple of collaborator instance and set of assigned names
        id_author_curr = -1 # Current author id
        gender_curr = -1 # Current gender
        dic_map_name_dois: Dict[str, Tuple[str, str]] = {}
        # Maps tuple of DOI and name to collaborator instance (will be returned)
        dic_map_doi_collaborator = {}
        with open(self._file_disambiguation, "r", encoding="utf-8") as file_disamb:
            csv_reader = DictReader(file_disamb)
            for row in csv_reader: # Iterate over rows in CSV
                # Retrieve data from row
                id_author_row = int(row["id"])
                name = TransferToCSV._clean_collaborator_name(row["name"])
                first_name_key = TransferToCSV._get_first_name_key(name)
                doi = row["doi"]

                if id_author_curr != id_author_row: # Test if we have a new author
                    # Disambiguate collected names
                    TransferToCSV._merge_names(dic_map_name_dois)

                    # Integrate the merged names
                    self._integrate_collaborators_merged(
                        dic_map_doi_collaborator=dic_map_doi_collaborator,
                        gender_curr=gender_curr,
                        dic_map_name_dois=dic_map_name_dois
                    )

                    # Re-init cache
                    id_author_curr = id_author_row
                    gender_curr = row["gender"]
                    dic_map_name_dois = { # New element
                        first_name_key: {(name, doi)}
                    }

                else: # Same author as last row
                    # Add the first name key to the set
                    if first_name_key not in dic_map_name_dois:
                        dic_map_name_dois[first_name_key] = set()
                    dic_map_name_dois[first_name_key].add((name, doi))

        # Integrate final name
        TransferToCSV._merge_names(dic_map_name_dois)
        self._integrate_collaborators_merged(
            dic_map_doi_collaborator=dic_map_doi_collaborator,
            gender_curr=gender_curr,
            dic_map_name_dois=dic_map_name_dois
        )

        return dic_map_doi_collaborator

    def _integrate_collaborator_from_json(
            self,
            json_collaborator: Dict[str, Any]) -> AuthorName:
        name = AuthorName(
            id_author_name=self._id_author_name,
            id_author=self._id_author,
            name=self._clean_collaborator_name(json_collaborator["name"])
        )
        author = Author(
            id_author=self._id_author,
            id_gender=MAP_GENDER_ID["unknown"],
            disambiguated=False
        )

        self.collection.authors.append(author)
        self.collection.author_names.append(name)

        self._id_author += 1
        self._id_author_name += 1
        return name

    def _integrate_journal_from_json(
        self, json_paper: Dict[str, Any]
    ) -> Journal:
        journal = Journal(
            id_journal=self._id_journal,
            code=json_paper["journal"]["id"],
            short=json_paper["journal"]["abbreviatedName"],
            name=json_paper["journal"]["name"],
            issue=json_paper["issue"]["number"],
            volume=int(json_paper["volume"]["number"]),
        )
        self.collection.journals.append(journal)
        self._id_journal += 1
        return journal

    def _write_invalid_metadata_log(self, l_metadata_failed: List[str]):
        with open(self._path_log, "w", encoding="utf-8") as file:
            file.write("\n".join(l_metadata_failed))

    def _write_to_csv(self):
        df_genders = pd.DataFrame(data=MAP_GENDER_ID.items(), columns=["gender", "id_gender"])
        df_genders = df_genders.set_index("id_gender")

        df_authors = pd.DataFrame(data=self.collection.authors)
        df_authors = df_authors.set_index("id_author")

        df_publications = pd.DataFrame(
            data=self.collection.publications)
        df_publications = df_publications.set_index("id_publication")

        df_author_names = pd.DataFrame(data=self.collection.author_names)
        df_author_names = df_author_names.set_index("id_author_name")

        df_authorships = pd.DataFrame(data=self.collection.authorships)
        df_authorships = df_authorships.set_index("id_authorship")

        df_journals = pd.DataFrame(
            data=self.collection.journals)
        df_journals = df_journals.set_index("id_journal")

        for df, file in zip(
                [df_genders, df_authors, df_author_names,
                df_publications, df_authorships, df_journals],
                ["genders", "authors", "author_names",
                 "publications", "authorships", "journals"]):
            path = os.path.join(self._folder_output, f"{file}.csv")
            print(f"Writing {file} to {path}.")
            df.to_csv(path)

    def integrate_data(self, n_projects: Union[int, None] = None) -> APSCollection:
        """Integrates the APS dataset.
        Follows the recipe:
        1. Integrate authors from disambiguation CSV.
        2. Iterate over all paper metadata.
            2.0 Store or assign journal
            2.1 If author can be matched via DOI, assign it to author
            2.2 Else create new non-disambiguated author (indicated via flag)

        Args:
            n_projects (Union[int, None], optional): Number of projects to integrate.
            Can be used for testing purposes to not include the whole data. Defaults to None.

        Returns:
            APSCollection: Collection holding the APS dataset.
        """
        print("Integrating disambiguated authors from CSV.")
        dic_map_doi_coll_disamb = self._integrate_collaborators_from_disamb()
        print((f"Integrated {len(self.collection.authors)} "
            "unique collaborators.\n"
            "Starting metadata integration (might take a while)."))
        if n_projects is not None:
            print(f"Will stop after {n_projects} publications.")

        cache_journals = {}

        cnt_non_disamb = 0
        l_metadata_failed = []

        for folder_journal in os.listdir(self._folder_metadata):
            path_journal = os.path.join(
                    self._folder_metadata,
                    folder_journal)
            if not os.path.isdir(path_journal):
                continue
            print(f"Working on {folder_journal}")
            for folder_volume in os.listdir(path_journal):
                path_volume = os.path.join(
                    self._folder_metadata,
                    folder_journal,
                    folder_volume)
                if not os.path.isdir(path_volume):
                    continue
                for file_path_paper in os.listdir(path_volume):
                    if not file_path_paper.endswith(".json"):
                        continue

                    # Paper metadata
                    json_paper = None
                    with open(os.path.join(path_volume, file_path_paper),
                            "r", encoding="utf-8") as file_paper:
                        json_paper = json.load(file_paper)

                    # Journal
                    tpl_journal = (
                        json_paper["journal"]["id"],
                        int(json_paper["volume"]["number"]),
                        json_paper["issue"]["number"],
                    )
                    journal = None
                    if tpl_journal not in cache_journals:
                        journal = self._integrate_journal_from_json(json_paper)
                        cache_journals[tpl_journal] = journal
                    else:
                        journal = cache_journals[tpl_journal]

                    # Add journal <-> paper link
                    project = self._integrate_project_from_json(
                        dic_from_json=json_paper, id_journal=journal.id_journal)

                    # Authors in paper
                    if "authors" not in json_paper:
                        l_metadata_failed.append(os.path.join(f"{journal.code}/{journal.volume}/", file_path_paper))
                        continue

                    # Collaborators
                    for json_collaborator in json_paper["authors"]:
                        if json_collaborator["type"] != "Person":
                            continue

                        if (project.doi, json_collaborator["name"]) in dic_map_doi_coll_disamb: # disambiguated
                            author_name = dic_map_doi_coll_disamb[(project.doi,\
                                json_collaborator["name"])]
                        else:
                            author_name = self._integrate_collaborator_from_json(
                                json_collaborator=json_collaborator)
                            cnt_non_disamb += 1

                        # Add author <-> publication link
                        authorship = Authorship(
                            id_authorship=self._id_authorship,
                            id_publication=project.id_publication,
                            id_author_name=author_name.id_author_name
                        )
                        self.collection.authorships.append(authorship)
                        self._id_authorship += 1

            if (n_projects is not None) and\
                    (len(self.collection.publications) + 1 >= n_projects):
                print(f"Quitting early after reading {len(self.collection.publications)} projects.")
                break
        if cnt_non_disamb > 0:
            print(f"({cnt_non_disamb}/{len(self.collection.authors)}) "
                  "collaborators were not disambiguated.")
        if len(l_metadata_failed) > 0:
            print(f"A total of {len(l_metadata_failed)} metadata files could not be read.")
            if self._path_log is not None:
                print(f"Writing list of invalid files to {self._path_log}.")
                self._write_invalid_metadata_log(l_metadata_failed)

        self._write_to_csv()
