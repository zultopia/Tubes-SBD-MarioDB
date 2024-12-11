from enum import Enum

class NodeType(Enum):
    PROJECT = "PROJECT"    
    JOIN = "JOIN"         
    SORTING = "SORTING"
    TABLE = "TABLE"
    SELECTION = "SELECTION"
    UNION_SELECTION = "UNION"
    UPDATE = "UPDATE"

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