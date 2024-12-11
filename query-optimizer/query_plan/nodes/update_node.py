from typing import Dict, List, Tuple
from ..base import QueryNode
from ..enums import NodeType

class UpdateNode(QueryNode):
    def __init__(self, updates: List[Tuple[str, str]]):  # [(column, new_value),...]
        super().__init__(NodeType.UPDATE)

        # sort the updates based on column name
        updates = sorted(updates, key=lambda u: u[0])

        self.updates = updates
        self.child = None
    
    def set_child(self, child: QueryNode):
        self.child = child
    
    def __str__(self) -> str:
        updates_str = ', '.join([f"{col} = {val}" for col, val in self.updates])
        return f"UPDATE {updates_str}"
    
    def _calculate_operation_cost(self, statistics: Dict) -> float:
        return 1
    
    def estimate_cost(self, statistics: Dict) -> float:
        return 1

    def clone(self):
        cloned_updates = self.updates.copy()
        cloned_node = UpdateNode(cloned_updates)
        if self.child:
            cloned_node.child = self.child.clone()
        cloned_node.id = self.id
        return cloned_node

