from .enums import Operator

class Condition:
    left_id: str
    left_table_name: str
    left_attribute: str

    right_id: str
    right_table_name: str
    right_attribute: str

    def __init__(self, left_operand: str, right_operand: str, operator: Operator, left_id: str = None, left_table_name: str = None, left_attribute: str = None):
        self.left_operand = left_operand
        self.right_operand = right_operand
        self.operator = operator

        # TODO: id, table_name, attribute harus diset kiri dan kanan


    def __str__(self) -> str:
        return f"{self.left_operand} {self.operator.value} {self.right_operand}"
    
    def is_constant_comparison(self) -> bool:
        # Check if right_operand is a constant (e.g., number or quoted string)
        # Could check if right_operand starts/ends with quotes or is numeric
        if self.right_operand[0] == "'" and self.right_operand[-1] == "'":
            return True
        
        try :
            float(self.right_operand)
            return True
        except ValueError:
            return False

    