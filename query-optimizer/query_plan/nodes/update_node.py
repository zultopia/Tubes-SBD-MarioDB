from typing import Dict, List, Tuple, Optional
from ..base import QueryNode
from ..enums import NodeType

class UpdateNode(QueryNode):
    def __init__(self, updates: List[Tuple[str, str]]):  # [(column, new_value),...]
        super().__init__(NodeType.UPDATE)
        self.updates = sorted(updates, key=lambda u: u[0])  # Sort updates by column name
        self.child = None  # Single child node
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

    def attributes(self) -> List[str]:
        """
        Returns the list of attributes from the child node.
        """
        if not self.child:
            raise ValueError("UpdateNode has no child.")
        if self._cached_attributes is None:
            self._cached_attributes = self.child.attributes().copy()
        return self._cached_attributes