from typing import List, Dict
from ..base import QueryNode
from ..enums import NodeType
import uuid

class ProjectNode(QueryNode):
    def __init__(self, attributes: List[str]):
        super().__init__(NodeType.PROJECT)
        # Sort the attributes to ensure consistent ordering
        attributes.sort()

        self.attributes = attributes

        self.child = None  # Single child node

    def set_child(self, child: QueryNode):
        self.child = child

    def clone(self) -> 'ProjectNode':
        cloned_attributes = self.attributes.copy()
        cloned_child = self.child.clone() if self.child else None
        cloned_node = ProjectNode(cloned_attributes)
        cloned_node.id = self.id

        if cloned_child:
            cloned_node.set_child(cloned_child)
        return cloned_node

    def estimate_cost(self, statistics: Dict) -> float:
        return self._calculate_operation_cost(statistics)

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        # Placeholder implementation for project cost
        return 1

    def __str__(self) -> str:
        return f"PROJECT {', '.join(self.attributes)}"