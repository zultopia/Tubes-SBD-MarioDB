from typing import List, Dict
from ..base import QueryNode
from ..enums import NodeType
import uuid

class SortingNode(QueryNode):
    def __init__(self, attributes: List[str], ascending: bool = True):
        super().__init__(NodeType.SORTING)
        attributes.sort()
        self.attributes = attributes
        self.ascending = ascending
        self.child = None  # Single child node
        self.children = None  # Not used in SortingNode

    def set_child(self, child: QueryNode):
        self.child = child

    def clone(self) -> 'SortingNode':
        cloned_attributes = self.attributes.copy()
        cloned_child = self.child.clone() if self.child else None
        cloned_node = SortingNode(cloned_attributes, self.ascending)
        cloned_node.id = self.id

        if cloned_child:
            cloned_node.set_child(cloned_child)
        return cloned_node

    def estimate_size(self, statistics: Dict, alias_dict):
        if not self.child:
            return
        self.child.estimate_size()

        # Todo: Ganti ID Kl perlu (?)
        self.attributes = self.child.attributes
        self.n = self.child.n
        self.b = self.child.b

    def estimate_cost(self, statistics: Dict, alias_dict) -> float:
        # Sorting tidak memakan IO
        return self.child.estimate_cost()

    def __str__(self) -> str:
        order = "ASC" if self.ascending else "DESC"
        return f"SORT BY {', '.join(self.attributes)} {order}"