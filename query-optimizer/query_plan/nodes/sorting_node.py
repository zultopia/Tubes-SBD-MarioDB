from typing import List, Dict, Optional
from ..base import QueryNode
from ..enums import NodeType
import uuid

class SortingNode(QueryNode):
    def __init__(self, attributes: List[str], ascending: bool = True):
        super().__init__(NodeType.SORTING)
        self.sort_attributes = sorted(attributes)  # Sort for consistency
        self.ascending = ascending
        self.child = None  # Single child node
        self._cached_attributes: Optional[List[str]] = None  # Cache for attributes

    def set_child(self, child: QueryNode):
        self.child = child

    def clone(self) -> 'SortingNode':
        cloned_node = SortingNode(self.sort_attributes.copy(), self.ascending)
        cloned_node.id = self.id
        if self.child:
            cloned_node.set_child(self.child.clone())
        return cloned_node

    def estimate_cost(self, statistics: Dict) -> float:
        return self._calculate_operation_cost(statistics)

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        # Placeholder for actual cost calculation based on sorting
        return 1.0

    def __str__(self) -> str:
        order = "ASC" if self.ascending else "DESC"
        return f"SORT BY {', '.join(self.sort_attributes)} {order}"

    def attributes(self) -> List[str]:
        """
        Returns the list of attributes from the child node.
        """
        if not self.child:
            raise ValueError("SortingNode has no child.")
        if self._cached_attributes is None:
            self._cached_attributes = self.child.attributes().copy()
        return self._cached_attributes