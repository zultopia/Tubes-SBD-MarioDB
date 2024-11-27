from typing import List, Literal, Dict
from ..base import QueryNode
from ..enums import NodeType, JoinAlgorithm
from utils import Pair

class JoinCondition:
    def __init__(self, left_attr: str, right_attr: str, 
                 operator: Literal['>', '>=', '<', '<=', '=']):
        self.left_attr = left_attr
        self.right_attr = right_attr
        self.operator = operator

class JoinNode(QueryNode):
    def __init__(self, algorithm: JoinAlgorithm = JoinAlgorithm.NESTED_LOOP):
        super().__init__(NodeType.JOIN)
        self.algorithm = algorithm
        self.children = None
    


    def set_children(self, children: Pair[QueryNode, QueryNode]):
        self.children = children

class ConditionalJoinNode(JoinNode):
    def __init__(self, algorithm: JoinAlgorithm = JoinAlgorithm.NESTED_LOOP, conditions: List[JoinCondition] = []):
        super().__init__(algorithm)
        self.conditions = conditions
    
    def estimate_cost(self, statistics: Dict) -> float:
        return 1

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        return 1



class NaturalJoinNode(JoinNode):
    def __init__(self, algorithm: JoinAlgorithm = JoinAlgorithm.NESTED_LOOP):
        super().__init__(algorithm)
    
    def estimate_cost(self, statistics: Dict) -> float:
        return 1

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        return 1
