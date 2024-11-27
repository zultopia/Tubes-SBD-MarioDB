from enum import Enum

class NodeType(Enum):
    PROJECT = "PROJECT"    
    JOIN = "JOIN"         
    SORTING = "SORTING"
    TABLE = "TABLE"

class JoinAlgorithm(Enum):
    NESTED_LOOP = "nested_loop"
    MERGE = "merge"
    HASH = "hash"