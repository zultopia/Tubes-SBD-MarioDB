from .enums import Operator




class Condition:
    def __init__(self, left_operand: str, right_operand: str, operator: Operator):
        # Use internal variables with different names to avoid recursion
        self._left_operand = left_operand
        self._right_operand = right_operand
        self.operator = operator
        
        self._left_table_alias = None
        self._left_attribute = None
        self._right_table_alias = None
        self._right_attribute = None

        # Initial parsing of operands
        self._parse_left_operand(left_operand)
        self._parse_right_operand(right_operand)

    def _parse_left_operand(self, value: str):
        """Helper method to parse left operand"""
        left = value.split('.')
        if len(left) == 2:
            self._left_table_alias = left[0]
            self._left_attribute = left[1]
        else:
            self._left_table_alias = None
            self._left_attribute = value

    def _parse_right_operand(self, value: str):
        """Helper method to parse right operand"""
        if value[0] == "'" and value[-1] == "'":
            self._right_table_alias = None
            self._right_attribute = None
            return
            
        try:
            float(value)
            self._right_table_alias = None
            self._right_attribute = None
        except ValueError:
            right = value.split('.')
            if len(right) == 2:
                self._right_table_alias = right[0]
                self._right_attribute = right[1]
            else:
                self._right_table_alias = None
                self._right_attribute = value
    
    def __str__(self) -> str:
        left = self.left_table_alias + '.' + self.left_attribute if self.left_table_alias else self.left_attribute
        right = self.right_table_alias + '.' + self.right_attribute if self.right_table_alias else self.right_attribute
        if self.is_constant_comparison():
            right = self.right_operand
        return f"{left} {self.operator.value} {right}"
    
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
    
    def __repr__(self) -> str:
        return self.__str__()

    @property
    def left_operand(self):
        return self._left_operand

    @left_operand.setter
    def left_operand(self, value):
        self._left_operand = value
        self._parse_left_operand(value)

    @property
    def left_table_alias(self):
        return self._left_table_alias

    @left_table_alias.setter
    def left_table_alias(self, value):
        self._left_table_alias = value
        self._left_operand = f"{value}.{self._left_attribute}" if value else self._left_attribute

    @property
    def left_attribute(self):
        return self._left_attribute

    @left_attribute.setter
    def left_attribute(self, value):
        self._left_attribute = value
        self._left_operand = f"{self._left_table_alias}.{value}" if self._left_table_alias else value

    @property
    def right_operand(self):
        return self._right_operand

    @right_operand.setter
    def right_operand(self, value):
        self._right_operand = value
        self._parse_right_operand(value)

    @property
    def right_table_alias(self):
        return self._right_table_alias

    @right_table_alias.setter
    def right_table_alias(self, value):
        self._right_table_alias = value
        if not self.is_constant_comparison():
            self._right_operand = f"{value}.{self._right_attribute}" if value else self._right_attribute

    @property
    def right_attribute(self):
        return self._right_attribute

    @right_attribute.setter
    def right_attribute(self, value):
        self._right_attribute = value
        if not self.is_constant_comparison():
            self._right_operand = f"{self._right_table_alias}.{value}" if self._right_table_alias else value