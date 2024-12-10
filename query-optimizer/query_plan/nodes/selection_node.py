from typing import List, Dict
from ..base import QueryNode
from ..enums import SelectionOperation, NodeType
from copy import deepcopy
import uuid


class SelectionCondition: 
    def __init__(self, left_operand: str, right_operand: str, operator: SelectionOperation):
        self.left_operand = left_operand
        self.right_operand = right_operand
        self.operator = operator

    def __str__(self) -> str:
        return f"{self.left_operand} {self.operator.value} {self.right_operand}"

class SelectionNode(QueryNode):
    def __init__(self, conditions: List[SelectionCondition]):
        super().__init__(NodeType.SELECTION)
        self.conditions = conditions
        self.child = None  # Single child node
        self.children = None  # Not used in SelectionNode

    def set_child(self, child: QueryNode):
        self.child = child

    def clone(self) -> 'SelectionNode':
        cloned_conditions = [SelectionCondition(c.left_operand, c.right_operand, c.operator) for c in self.conditions]
        cloned_node = SelectionNode(cloned_conditions)
        cloned_node.id = self.id

        if self.child:
            cloned_node.set_child(self.child.clone())
        return cloned_node

    def estimate_cost(self, statistics: Dict) -> float:
        return self._calculate_operation_cost(statistics)

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        return 1

    def __str__(self) -> str:
        return f"SELECT {', '.join([str(c) for c in self.conditions])}"

class UnionSelectionNode(QueryNode):
    def __init__(self, children: List[SelectionNode]):
        super().__init__(NodeType.UNION_SELECTION)
        self.children = children  

    def set_child_to_all(self, child: QueryNode):
        """Sets the given child node to all selection nodes in the union"""
        for selection_node in self.children:
            selection_node.set_child(child.clone())  

    def clone(self) -> 'UnionSelectionNode':
        cloned_children = [child.clone() for child in self.children]
        cloned_node = UnionSelectionNode(cloned_children)
        cloned_node.id = self.id

        return cloned_node

    def estimate_cost(self, statistics: Dict) -> float:
        return self._calculate_operation_cost(statistics)

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        # Placeholder implementation for union selection cost
        return 1

    def __str__(self) -> str:
        return f"UNION"

    def set_child_to_all(self, child: QueryNode):
        for selection_node in self.children:
            selection_node.set_child(child.clone())
    
    