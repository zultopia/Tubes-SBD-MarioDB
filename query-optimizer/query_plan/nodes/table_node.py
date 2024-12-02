from typing import Dict
from ..base import QueryNode
from ..enums import NodeType

class TableNode(QueryNode):
    alias: str
    table_name: str

    def __init__(self, table_name: str, alias: str | None = None):
        super().__init__(NodeType.TABLE)
        self.table_name = table_name
        self.alias = table_name if alias is None else alias
        self.children = None
    
    def estimate_cost(self, statistics: Dict) -> float:
        return 1

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        return 1

    def __str__(self) -> str:
        return f"TABLE {self.table_name} AS {self.alias}" if self.alias != self.table_name else f"TABLE {self.table_name}"
    
    