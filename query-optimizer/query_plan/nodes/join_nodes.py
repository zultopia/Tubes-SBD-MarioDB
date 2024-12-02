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
    
    def clone(self) -> 'JoinNode':
        ret: 'JoinNode'
        ret = JoinNode(self.algorithm)
        ret.set_children(self.children)
        return ret
        





class ConditionalJoinNode(JoinNode):
    def __init__(self, algorithm: JoinAlgorithm = JoinAlgorithm.NESTED_LOOP, conditions: List[JoinCondition] = []):
        super().__init__(algorithm)
        self.conditions = conditions
    
    def estimate_cost(self, statistics: Dict) -> float:
        return 1

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        return 1

    def __str__(self) -> str:
        return f"JOIN [{self.algorithm.value}]" if len(self.conditions) == 0 else f"JOIN [{self.algorithm.value}] ON {', '.join([f'{c.left_attr} {c.operator} {c.right_attr}' for c in self.conditions])}"

    def clone(self) -> 'ConditionalJoinNode':
        ret = ConditionalJoinNode(self.algorithm, self.conditions)
        ret.set_children(self.children)
        return ret


class NaturalJoinNode(JoinNode):
    def __init__(self, algorithm: JoinAlgorithm = JoinAlgorithm.NESTED_LOOP):
        super().__init__(algorithm)
    
    def estimate_cost(self, statistics: Dict) -> float:
        return 1

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        return 1

    def __str__(self) -> str:
        return f"NATURAL JOIN [{self.algorithm.value}]"
    
    def clone(self) -> 'NaturalJoinNode':
        ret = NaturalJoinNode(self.algorithm)
        ret.set_children(self.children)
        return ret
