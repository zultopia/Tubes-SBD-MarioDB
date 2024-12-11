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

    def estimate_cost(self, statistics: Dict) -> float:
        return self._calculate_operation_cost(statistics)

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        # Placeholder implementation for sorting cost
        return 1

    def __str__(self) -> str:
        order = "ASC" if self.ascending else "DESC"
        return f"SORT BY {', '.join(self.attributes)} {order}"