from enum import Enum

class NodeType(Enum):
    PROJECT = "PROJECT"    
    JOIN = "JOIN"         
    SORTING = "SORTING"
    TABLE = "TABLE"
    SELECTION = "SELECTION"
    UNION_SELECTION = "UNION"
    UPDATE = "UPDATE"
    LIMIT = "LIMIT"

class JoinAlgorithm(Enum):
    NESTED_LOOP = "nested_loop"
    MERGE = "merge"
    HASH = "hash"

class Operator(Enum):
    GREATER = '>'
    GREATER_EQ = '>='
    LESS = '<'
    LESS_EQ = '<='
    EQ = '='
    NEQ = '<>'

    @classmethod
    def from_string(cls, op_str: str) -> 'Operator':
        """
        Converts a string representation of an operator to its corresponding Enum member.
        
        Args:
            op_str (str): The operator as a string (e.g., '=', '>=').
        
        Returns:
            Operator: The corresponding Operator enum member.
        
        Raises:
            ValueError: If the operator string does not match any Enum member.
        """
        for op in cls:
            if op.value == op_str:
                return op
        raise ValueError(f"Unknown operator: {op_str}")