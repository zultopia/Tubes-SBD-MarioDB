from typing import List, Dict
from ..base import QueryNode
from ..enums import SelectionOperation

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
