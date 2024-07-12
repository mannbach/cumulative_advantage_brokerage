"""APS integration.
"""
import os
from csv import DictReader
from datetime import datetime
from typing import List, Set, Union, Dict

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session
from sqlalchemy import select

from .collection import APSCollection
from .models.collaboration import Collaboration
from .models.collaborator import Collaborator
from .models.collaborator_name import CollaboratorName
from .models.project import Project
from .models.gender import Gender,\
    GENDER_UNKNOWN, GENDER_FEMALE, GENDER_MALE
from .models.citation import Citation

class APSIntegrator:
    """Performs the integration of the APS dataset.
    See `APSIntegrator.integrate_data` for details.
    """
    _folder_aps_csv: str
    _file_gender: str
    _file_authors: str
    _file_author_names: str
    _file_authorships: str
    _file_publications: str
    _file_citations: str

    _s_id_collaborators: Set[int]
    _s_id_projects: Set[int]

    _d_map_name_collaborator: Dict[int, int]

    collection: Union[APSCollection, None] = None

    def __init__(
            self,
            engine: Engine,
            folder_csv: str,
            file_gender: str,
            file_authors: str,
            file_author_names: str,
            file_authorships: str,
            file_publications: str,
            file_citations: str,
            path_log: Union[str, None] = None) -> None:
        self.engine = engine
        self.path_log = path_log
        self._folder_aps_csv = folder_csv
        self._file_gender = file_gender
        self._file_authors = file_authors
        self._file_author_names = file_author_names
        self._file_authorships = file_authorships
        self._file_publications = file_publications
        self._file_citations = file_citations

        self._s_id_collaborators = set()
        self._s_id_projects = set()

        self._d_map_name_collaborator = dict()

        self.collection = APSCollection() # Initialize empty

    def integrate_genders(self) -> List[Gender]:
        genders = [GENDER_UNKNOWN, GENDER_FEMALE, GENDER_MALE]
        self.collection.genders = genders
        return genders

    def integrate_collaborators(self) -> List[Collaborator]:
        assert self.collection.genders is not None

        path_collaborators = os.path.join(self._folder_aps_csv, self._file_authors)
        print(f"Integrating collaborators from {path_collaborators}")

        self.collection.collaborators = []

        with open(path_collaborators, "r", encoding="utf-8") as file:
            reader = DictReader(file, )
            for row in reader:
                if row["disambiguated"] == "False":
                    continue
                collab = Collaborator(
                    id=int(row["id_author"]),
                    id_gender=int(row["id_gender_nq"])
                )
                self.collection.collaborators.append(collab)
                self._s_id_collaborators.add(collab.id)

        return self.collection.collaborators

    def integrate_collaborator_names(self) -> List[CollaboratorName]:
        assert self.collection.collaborators is not None

        path_collaborator_names = os.path.join(self._folder_aps_csv, self._file_author_names)
        print(f"Reading collaborator_names from {path_collaborator_names}")

        self.collection.collaborator_names = []

        with open(path_collaborator_names, "r", encoding="utf-8") as file:
            reader = DictReader(file)
            for row in reader:
                if not int(row["id_author"]) in self._s_id_collaborators:
                    continue
                name = CollaboratorName(
                    id=int(row["id_author_name"]),
                    id_collaborator=int(row["id_author"]),
                    name=row["name"])
                self._d_map_name_collaborator[name.id] = name.id_collaborator
                self.collection.collaborator_names.append(name)

        return self.collection.collaborator_names

    def integrate_collaborations(self) -> List[Collaboration]:
        assert self.collection.collaborators is not None
        assert self.collection.collaborator_names is not None

        path_collaborations = os.path.join(self._folder_aps_csv, self._file_authorships)
        print(f"Reading collaborations from {path_collaborations}")

        self.collection.collaborations = []

        with open(path_collaborations, "r", encoding="utf-8") as file:
            reader = DictReader(file)
            for row in reader:
                if int(row["id_author_name"]) not in self._d_map_name_collaborator:
                    continue
                collab = Collaboration(
                    id=int(row["id_authorship"]),
                    id_collaborator=self._d_map_name_collaborator[int(row["id_author_name"])],
                    id_project=int(row["id_publication"]),
                    id_collaborator_name=int(row["id_author_name"]))
                self._s_id_projects.add(collab.id_project)
                self.collection.collaborations.append(collab)

        return self.collection.collaborations

    def integrate_projects(self) -> List[Project]:
        assert self.collection.collaborations is not None

        path_projects = os.path.join(self._folder_aps_csv, self._file_publications)
        print(f"Reading projects from {path_projects}")

        self.collection.projects = []

        with open(path_projects, "r", encoding="utf-8") as file:
            reader = DictReader(file)
            for row in reader:
                if not int(row["id_publication"]) in self._s_id_projects:
                    continue
                project = Project(
                    id=int(row["id_publication"]),
                    timestamp=datetime.fromisoformat(row["timestamp"]),
                    doi=row["doi"])
                self.collection.projects.append(project)

        return self.collection.projects

    def integrate_citations(self) -> List[Citation]:
        if len(self._s_id_projects) == 0:
            print("Load project ids")
            self._load_id_projects()

        path_citations = os.path.join(self._folder_aps_csv, self._file_citations)
        print(f"Reading citations from {path_citations}")

        self.collection.citations = []

        with open(path_citations, "r", encoding="utf-8") as file:
            reader = DictReader(file)
            for row in reader:
                id_citing, id_cited = (int(row[key]) for key in ("id_publication_citing", "id_publication_cited"))
                if not ((id_citing in self._s_id_projects) and (id_cited in self._s_id_projects)):
                    continue
                citation = Citation(
                    id_project_citing=id_citing,
                    id_project_cited=id_cited,
                )
                self.collection.citations.append(citation)

        return self.collection.citations

    def populate_database(self):
        """Collects all data by integrator methods and then populates the database accordingly.
        """
        genders = self.integrate_genders()
        collaborators = self.integrate_collaborators()
        collaborator_names = self.integrate_collaborator_names()
        collaborations = self.integrate_collaborations()
        projects = self.integrate_projects()
        citations = self.integrate_citations()

        print("Storing data in database.")
        with Session(self.engine) as session:
            session.add_all(genders)
            session.commit()
            session.add_all(collaborators)
            session.commit()
            session.add_all(collaborator_names)
            session.add_all(projects)
            session.commit()
            session.add_all(collaborations)
            session.commit()
            session.add_all(citations)
            session.commit()

    def _load_id_projects(self):
        with Session(self.engine) as session:
            self._s_id_projects = {
                r.id for r in session.execute(select(Project.id))
            }
