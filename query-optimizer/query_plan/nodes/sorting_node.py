from typing import List, Dict
from ..base import QueryNode
from ..enums import NodeType

class SortingNode(QueryNode):
    ascending: bool

    def __init__(self, attributes: List[str], ascending: bool = True):
        super().__init__(NodeType.SORTING)
        self.attributes = attributes
        self.children = None
        self.ascending = ascending
    
    def set_child(self, child: QueryNode):
        self.child = child
    
    def estimate_cost(self, statistics: Dict) -> float:
        return 1

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        return 1

    def __str__(self) -> str:
        return f"SORT BY {', '.join(self.attributes)} ASC" if self.ascending else f"SORT BY {', '.join(self.attributes)} DESC"