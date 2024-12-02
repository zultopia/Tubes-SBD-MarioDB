from typing import List, Dict
from ..base import QueryNode
from ..enums import SelectionOperation, NodeType





class SelectionCondition: 
    left_operand: str
    right_operand: str
    operator: SelectionOperation

    def __init__(self, left_operand: str, right_operand: str, operator: SelectionOperation):
        self.left_operand = left_operand
        self.right_operand = right_operand
        self.operator = operator

    def __str__(self) -> str:
        return f"{self.left_operand} {self.operator.value} {self.right_operand}"
    

class SelectionNode(QueryNode):
    # Conjunction of conditions
    conditions: List[SelectionCondition]

    def __init__(self, conditions: List[SelectionCondition]):
        self.conditions = conditions
        self.child = None
    
    def set_child(self, child: QueryNode):
        self.child = child
    
    def estimate_cost(self, statistics: Dict) -> float:
        return 1

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        return 1

    def __str__(self) -> str:
        return f"SELECT {', '.join([str(c) for c in self.conditions])}"

    def clone(self) -> 'SelectionNode':
        ret = SelectionNode(self.conditions)
        ret.set_child(self.child)
        return ret

class UnionSelectionNode(QueryNode):
    def __init__(self, children: List[SelectionNode]):
        super().__init__(NodeType.UNION_SELECTION)  # Make sure to add this to NodeType enum
        self.children = children
    
    def set_child_to_all(self, child: QueryNode):
        """Sets the given child node to all selection nodes in the union"""
        for selection_node in self.children:
            selection_node.set_child(child)
    
    def estimate_cost(self, statistics: Dict) -> float:
        return 1

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        return 1

    def __str__(self) -> str:
        return f"UNION"

    def clone(self) -> 'UnionSelectionNode':
        ret = UnionSelectionNode(self.children)
        return ret
    
    