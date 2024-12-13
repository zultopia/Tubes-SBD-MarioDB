from typing import Dict, List, Tuple, Optional
from QueryOptimizer.query_plan.base import QueryNode
from QueryOptimizer.query_plan.enums import NodeType
from QueryOptimizer.utils import Pair

class UpdateNode(QueryNode):
    def __init__(self, updates: List[Tuple[str, str]]):  # [(column, new_value),...]
        super().__init__(NodeType.UPDATE)
        self.updates = sorted(updates, key=lambda u: u[0])  # Sort updates by column name
        self.child = None  # Single child node
        self.children = None
        self._cached_attributes: Optional[List[str]] = None  # Cache for attributes

    def set_child(self, child: QueryNode):
        self.child = child

    def clone(self) -> 'UpdateNode':
        cloned_node = UpdateNode(self.updates.copy())
        cloned_node.id = self.id
        if self.child:
            cloned_node.set_child(self.child.clone())
        return cloned_node

    def estimate_cost(self, statistics: Dict) -> float:
        return self._calculate_operation_cost(statistics)

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        # Placeholder for actual cost calculation
        return 1.0

    def __str__(self) -> str:
        updates_str = ', '.join([f"{col} = {val}" for col, val in self.updates])
        return f"UPDATE {updates_str}"
    
    def estimate_size(self, statistics, alias_dict) -> float:
        return 0
    
    def estimate_cost(self, statistics: Dict, alias_dict) -> float:
        return 0

    def clone(self):
        cloned_updates = self.updates.copy()
        cloned_node = UpdateNode(cloned_updates)
        if self.child:
            cloned_node.child = self.child.clone()
        cloned_node.id = self.id
        return cloned_node

    def get_node_attributes(self) -> List[Pair[str, str]]:
        """
        Returns the list of attributes from the child node.
        """
        if not self.child:
            raise ValueError("UpdateNode has no child.")
        if self._cached_attributes is None:
            self._cached_attributes = self.child.get_node_attributes().copy()
        return self._cached_attributes