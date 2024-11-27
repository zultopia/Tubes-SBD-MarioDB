from typing import Dict
from ..base import QueryNode
from ..enums import NodeType

class TableNode(QueryNode):
    def __init__(self, table_name: str):
        super().__init__(NodeType.TABLE)
        self.table_name = table_name
        self.children = None
    
    def estimate_cost(self, statistics: Dict) -> float:
        return 1

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        return 1
    
    