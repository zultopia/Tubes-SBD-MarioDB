from typing import Dict
from ..base import QueryNode
from ..enums import NodeType
import uuid

class TableNode(QueryNode):
    def __init__(self, table_name: str, alias: str | None = None):
        super().__init__(NodeType.TABLE)
        self.table_name = table_name
        self.alias = table_name if alias is None else alias
        self.child = None  # Single child node
        self.children = None  # Not used in TableNode

    def clone(self) -> 'TableNode':
        cloned_node = TableNode(self.table_name, self.alias)
        cloned_node.id = self.id

        if self.child:
            cloned_node.set_child(self.child.clone())
        return cloned_node

    def set_child(self, child: QueryNode):
        self.child = child

    def estimate_cost(self, statistics: Dict) -> float:
        return self._calculate_operation_cost(statistics)

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        # Placeholder implementation for table scan cost
        return 1

    def __str__(self) -> str:
        return f"TABLE {self.table_name} AS {self.alias}" if self.alias != self.table_name else f"TABLE {self.table_name}"