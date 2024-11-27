from typing import List, Dict
from QueryOptimizer.query_plan.base import QueryNode
from QueryOptimizer.query_plan.enums import NodeType

class ProjectNode(QueryNode):
    def __init__(self, condition: List[str]):
        super().__init__(NodeType.PROJECT)
        self.condition = condition
        self.child = None
    
    def set_child(self, child: QueryNode):
        self.child = child
    
    def estimate_cost(self, statistics: Dict) -> float:
        return 1

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        return 1