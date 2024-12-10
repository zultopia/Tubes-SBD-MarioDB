from typing import List, Literal, Dict
from ..base import QueryNode
from ..enums import NodeType, JoinAlgorithm
from utils import Pair
from ..shared import Condition

class JoinNode(QueryNode):
    def __init__(self, algorithm: JoinAlgorithm = JoinAlgorithm.NESTED_LOOP):
        super().__init__(NodeType.JOIN)
        self.algorithm = algorithm
        self.children = None  # Should be of type Pair[QueryNode, QueryNode]

    def set_children(self, children: Pair[QueryNode, QueryNode]):
        self.children = children

    def clone(self) -> 'JoinNode':
        cloned_children = None
        if self.children:
            cloned_children = Pair(self.children.first.clone(), self.children.second.clone())
        cloned_node = JoinNode(self.algorithm)
        cloned_node.id = self.id
        cloned_node.set_children(cloned_children)
        return cloned_node

    def estimate_cost(self, statistics: Dict) -> float:
        return self._calculate_operation_cost(statistics)

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        # Placeholder implementation
        return 1

    def __str__(self) -> str:
        return f"JOIN [{self.algorithm.value}]"





class ConditionalJoinNode(JoinNode):
    def __init__(self, algorithm: JoinAlgorithm = JoinAlgorithm.NESTED_LOOP, conditions: List[Condition] = None):
        super().__init__(algorithm)
        self.node_type = NodeType.JOIN
        self.conditions = conditions if conditions is not None else []

    def clone(self) -> 'ConditionalJoinNode':
        cloned_conditions = [Condition(c.left_attr, c.right_attr, c.operator) for c in self.conditions]
        cloned_children = None
        
        
        if self.children:
            cloned_children = Pair(self.children.first.clone(), self.children.second.clone())
        cloned_node = ConditionalJoinNode(self.algorithm, cloned_conditions)
        cloned_node.id = self.id

        cloned_node.set_children(cloned_children)

        return cloned_node

    def estimate_cost(self, statistics: Dict) -> float:
        return self._calculate_operation_cost(statistics)

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        # Placeholder implementation for conditional joins
        return 1

    def __str__(self) -> str:
        if not self.conditions:
            return f"JOIN [{self.algorithm.value}]"
        conditions_str = ', '.join([f"{c.left_attr} {c.operator} {c.right_attr}" for c in self.conditions])
        return f"JOIN [{self.algorithm.value}] ON {conditions_str}"


class NaturalJoinNode(JoinNode):
    def __init__(self, algorithm: JoinAlgorithm = JoinAlgorithm.NESTED_LOOP):
        super().__init__(algorithm)
        self.node_type = NodeType.JOIN

    def clone(self) -> 'NaturalJoinNode':
        cloned_children = None
        if self.children:

            cloned_children = Pair(self.children.first.clone(), self.children.second.clone())
        cloned_node = NaturalJoinNode(self.algorithm)
        cloned_node.id = self.id

        cloned_node.set_children(cloned_children)
        return cloned_node

    def estimate_cost(self, statistics: Dict) -> float:
        return self._calculate_operation_cost(statistics)

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        # Placeholder implementation for natural joins
        return 1

    def __str__(self) -> str:
        return f"NATURAL JOIN [{self.algorithm.value}]"
