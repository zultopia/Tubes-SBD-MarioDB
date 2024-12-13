from .enums import Operator

class Condition:
    left_table_alias: str
    left_attribute: str

    right_table_alias: str
    right_attribute: str

    def __init__(self, left_operand: str, right_operand: str, operator: Operator, left_id: str = None, left_table_name: str = None, left_attribute: str = None):
        self.left_operand = left_operand
        self.right_operand = right_operand
        self.operator = operator

        left = left_operand.split('.')
        right = right_operand.split('.')
        if len(left) == 2:
            self.left_table_alias = left[0]
            self.left_attribute = left[1]
        else:
            self.left_table_alias = left_table_name
            self.left_attribute = left_operand

        #  handle 3.14
        if right_operand[0] == "'" and right_operand[-1] == "'":
            self.right_table_alias = None
            self.right_attribute = None
        else:
            #  handle 3.14 but also handles table.attribute
            try:
                float(right_operand)
                self.right_table_alias = None
                self.right_attribute = None
            except ValueError:
                right = right_operand.split('.')
                if len(right) == 2:
                    self.right_table_alias = right[0]
                    self.right_attribute = right[1]
                else:
                    self.right_table_alias = left_table_name
                    self.right_attribute = right_operand


        # TODO: attribute & table_alias harus diset (kiri dan kanan)


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

    