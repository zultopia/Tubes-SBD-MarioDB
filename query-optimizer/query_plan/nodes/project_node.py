from typing import List, Dict
from ..base import QueryNode
from ..enums import NodeType

class ProjectNode(QueryNode):
    attributes: List[str]

    def __init__(self, attributes: List[str]):
        self.attributes = attributes
        self.child = None
    
    def set_child(self, child: QueryNode):
        self.child = child
    
    def estimate_cost(self, statistics: Dict) -> float:
        return 1

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        return 1

    def __str__(self) -> str:
        return f"PROJECT {', '.join(self.attributes)}"

    def clone(self) -> 'ProjectNode':
        ret = ProjectNode(self.attributes)
        ret.set_child(self.child)
        return ret
