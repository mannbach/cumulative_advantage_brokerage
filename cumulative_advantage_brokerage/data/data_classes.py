from dataclasses import dataclass, field
from datetime import datetime
from typing import List

import pandas as pd

@dataclass
class Author:
    id_author: int
    id_gender: int
    disambiguated: bool

@dataclass
class AuthorName:
    id_author_name: int
    id_author: int
    name: str
    # doi: str

@dataclass
class Publication:
    id_publication: int
    id_journal: int
    timestamp: datetime
    doi: str

@dataclass
class Authorship:
    id_authorship: int
    id_publication: int
    id_author_name: int

@dataclass
class Journal:
    id_journal: int
    code: str
    short: str
    name: str
    issue: str
    volume: int

@dataclass
class APSCollection:
    authors: List[Author] = field(default_factory=list)
    author_names: List[AuthorName] = field(default_factory=list)
    publications: List[Publication] = field(default_factory=list)
    authorships: List[Authorship] = field(default_factory=list)
    journals: List[Journal] = field(default_factory=list)

@dataclass
class APSDataFrames:
    genders: pd.DataFrame
    authors: pd.DataFrame
    author_names: pd.DataFrame
    publications: pd.DataFrame
    authorships: pd.DataFrame
    journals: pd.DataFrame
    citations: pd.DataFrame
