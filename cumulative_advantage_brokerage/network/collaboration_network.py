from collections import defaultdict
from typing import Any, abstractmethod, Callable, Dict, List, Tuple, Set

from .sql_edge_generator import SQLEdgeGenerator
from ..constants import\
    CN_EVENT_NODE_ADD_BEFORE,CN_EVENT_NODE_ADD_AFTER,\
    CN_EVENT_LINK_ADD_BEFORE,CN_EVENT_LINK_ADD_AFTER,\
    CN_EVENT_PROJECT_ADD_BEFORE,CN_EVENT_PROJECT_ADD_AFTER,\
    CN_EVENT_DATE_ADD_BEFORE,CN_EVENT_DATE_ADD_AFTER

class CollaborationNetwork:
    network: Any
    generator: SQLEdgeGenerator

    _event_listeners: Dict[str, Callable[[Any], Any]]

    def __init__(self, generator: SQLEdgeGenerator) -> None:
        self._event_listeners: Dict[str, List[Callable[[Any], Any]]] = defaultdict(list)
        self.generator = generator

    @classmethod
    @abstractmethod
    def from_generator(cls, generator: SQLEdgeGenerator):
        raise NotImplementedError

    def register_event_handler(self, event: str, event_handler: Callable[[Any], Any]):
        self._event_listeners[event].append(event_handler)

    def add_node(self, node: Any, node_attr: Any) -> int:
        self._execute_event_handlers(
            node=node, node_attr=node_attr,
            event=CN_EVENT_NODE_ADD_BEFORE)
        node = self._add_node(node=node, node_attr=node_attr)
        self._execute_event_handlers(
            node=node, node_attr=node_attr,
            event=CN_EVENT_NODE_ADD_AFTER)
        return node

    def add_link(self, link: Tuple[int, int], link_attr: Any) -> Tuple[int, int]:
        self._execute_event_handlers(
            link=link, link_attr=link_attr,
            event=CN_EVENT_LINK_ADD_BEFORE)
        link = self._add_link(link=link, link_attr=link_attr)
        self._execute_event_handlers(
            link=link, link_attr=link_attr,
            event=CN_EVENT_LINK_ADD_AFTER)
        return link

    def add_project(self, project: Set[int], project_attr: Any) -> Set[int]:
        self._execute_event_handlers(
            project=project, project_attr=project_attr,
            event=CN_EVENT_PROJECT_ADD_BEFORE)
        project = self._add_project(
            project=project, project_attr=project_attr)
        self._execute_event_handlers(
            project=project, project_attr=project_attr,
            event=CN_EVENT_PROJECT_ADD_AFTER)
        return project

    def add_date(self, date: Any) -> Any:
        self._execute_event_handlers(
            date=date, event=CN_EVENT_DATE_ADD_BEFORE)
        date = self._add_date(date=date)
        self._execute_event_handlers(
            date=date, event=CN_EVENT_DATE_ADD_AFTER)
        return date

    @abstractmethod
    def _add_node(self, node: Any, node_attr: Any) -> int:
        raise NotImplementedError

    @abstractmethod
    def _add_link(self, link: Tuple[int, int], link_attr: Any) -> Tuple[int, int]:
        raise NotImplementedError

    @abstractmethod
    def _add_project(self, project: Set[int], project_attr: Any) -> Set[int]:
        raise NotImplementedError

    @abstractmethod
    def _add_date(self, date: Any) -> Any:
        raise NotImplementedError

    def _execute_event_handlers(self, event: str, **kwargs):
        for f_handler in self._event_listeners[event]:
            f_handler(**kwargs)
