"""Growing temporal network.
"""
from datetime import datetime
from dataclasses import dataclass
from typing import Iterator, Dict, Set, List, Tuple, Any
from itertools import combinations_with_replacement

from .collaboration_network import CollaborationNetwork
from .sql_edge_generator import\
    SQLEdgeGenerator, DateYield, CollaboratorYield

@dataclass
class NodeAttributes:
    id_gender: int

@dataclass
class ProjectAttribute:
    id_project: int
    timestamp: datetime

@dataclass
class LinkAttribute:
    id_collaboration_u: int
    id_collaboration_v: int
    id_project: int
    timestamp: datetime

class GrowingTemporalLinkedListNetwork(CollaborationNetwork):
    """Class to generate temporally growing linked list networks.
    """
    network: Dict[int, Set[int]]
    nodes: Dict[int, NodeAttributes]
    edges: Dict[Tuple[int, int], List[LinkAttribute]]

    def __init__(self, generator: SQLEdgeGenerator) -> None:
        super().__init__(generator=generator)
        self.network = dict()
        self.edges = dict()
        self.nodes = dict()

    def generate_network(self) -> Iterator[DateYield]:
        """Generate network by iteratively adding edges.
        Edges are added in a group for each date.
        """
        for date in self.generator.edges():
            yield self.add_date(date=date)

    def aggregate_network(self) -> None:
        """Aggregate the whole network.
        """
        for _ in self.generate_network():
            continue

    def _add_date(self, date: DateYield) -> DateYield:
        for id_collab, attr_collab in date.collaborators.items():
            if id_collab not in self.network:
                self.add_node(node=id_collab, node_attr=attr_collab)
        for id_project, project in date.collaborations.items():
            self.add_project(
                project=project,
                project_attr=ProjectAttribute(
                    id_project=id_project,
                    timestamp=date.timestamp))
        return date

    def _add_project(self, project: Dict[int, int], project_attr: ProjectAttribute) -> Any:
        for node_u, node_v in combinations_with_replacement(project.keys(), 2):
            if node_u == node_v:
                continue
            # Using project.keys() iteration ensures in order passage
            # Before, a direct set iteration could cause the tuple to be unordered
            # To prevent future bugs, we enforce the ordering here
            node_u, node_v = (node_u, node_v) if node_u < node_v else (node_v, node_u)
            self.add_link(
                link=(node_u, node_v),
                link_attr=LinkAttribute(
                    id_collaboration_u=project[node_u],
                    id_collaboration_v=project[node_v],
                    id_project=project_attr.id_project,
                    timestamp=project_attr.timestamp))

    def _add_link(self, link: Tuple[int, int], link_attr: LinkAttribute) -> Any:
        node_u, node_v = link
        if node_v not in self.network[node_u]:
            self.edges[link] = []
        self.edges[link].append(link_attr)
        self.network[node_u].add(node_v)
        self.network[node_v].add(node_u)
        return link

    def _add_node(self, node: int, node_attr: CollaboratorYield) -> int:
        self.network[node] = set()
        self.nodes[node] = NodeAttributes(node_attr.id_gender)
        return node
