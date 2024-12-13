from typing import List, Dict, Optional
from ..base import QueryNode
from ..enums import NodeType
import uuid
from utils import Pair

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
        return f"SORT BY {', '.join(self.sort_attributes)} {order}"

    def get_node_attributes(self) -> List[Pair[str, str]]:
        """
        Returns the list of attributes from the child node.
        """
        if not self.child:
            raise ValueError("SortingNode has no child.")
        if self._cached_attributes is None:
            self._cached_attributes = self.child.attributes().copy()
        return self._cached_attributes