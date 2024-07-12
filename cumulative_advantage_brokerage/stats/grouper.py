from typing import NamedTuple, Callable, Optional, Union, Any, Generator, List, Tuple

from sqlalchemy import select, and_, func, literal
from sqlalchemy.orm import Query
import pandas as pd
import numpy as np

from ..dbm import GENDER_FEMALE, GENDER_MALE

class Grouper(NamedTuple):
    name: str = "none_grouper"
    add_constraints: Callable[[select, Any], List[Any]] = lambda q_base, grouping_key: []
    add_constraints_cached: Callable[[pd.DataFrame, Any], pd.Series] = lambda df, grouping_key: pd.Series(True, index=df.index)
    possible_values: List[Any] = []

GrouperDummy = Grouper(
    name="dummy",
    add_constraints=lambda q_base, grouping_key: [q_base.c.g_dummy == grouping_key],
    add_constraints_cached=lambda df, grouping_key: df["g_dummy"] == grouping_key,
    possible_values=["0"])
GrouperRole = Grouper(
    name="role",
    add_constraints=lambda q_base, grouping_key: [q_base.c.role == grouping_key],
    add_constraints_cached=lambda df, grouping_key: df["role"] == grouping_key,
    possible_values=["a", "b", "c"])
GrouperGender = Grouper(
    name="gender",
    add_constraints=lambda q_base, grouping_key:\
        [q_base.c.gender == grouping_key],
    add_constraints_cached=lambda df, grouping_key: df["gender"] == grouping_key,
    possible_values=[GENDER_FEMALE.gender, GENDER_MALE.gender])
GrouperBirthDecade = Grouper(
    name="birth_decade",
    add_constraints=lambda q_base, grouping_key:\
        [q_base.c.decade_birth == grouping_key],
    add_constraints_cached=lambda df, grouping_key: df["decade_birth"] == grouping_key,
    possible_values=np.arange(192, 202, dtype=int).tolist())
