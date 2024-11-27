from typing import List, Dict
from QueryOptimizer.query_plan.base import QueryNode
from QueryOptimizer.query_plan.enums import NodeType

class SortingNode(QueryNode):
    def __init__(self, attributes: List[str]):
        super().__init__(NodeType.SORTING)
        self.attributes = attributes
        self.children = None
    
    def set_child(self, child: QueryNode):
        self.child = child
    
    def estimate_cost(self, statistics: Dict) -> float:
        return 1

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        return 1