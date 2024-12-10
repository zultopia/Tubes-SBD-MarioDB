from .enums import Operator

class Condition: 
    def __init__(self, left_operand: str, right_operand: str, operator: Operator):
        self.left_operand = left_operand
        self.right_operand = right_operand
        self.operator = operator

    def __str__(self) -> str:
        return f"{self.left_operand} {self.operator.value} {self.right_operand}"
    