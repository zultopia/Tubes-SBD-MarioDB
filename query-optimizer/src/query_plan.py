from enum import Enum

"""
Complete documentation of what this file is about

1. Query plan is a tree based data structure, right?
2. But what does each node represent?

3. Here, my plan is that a node represents a so called "NodeType" which is an enumeration of possible node types in a query execution plan tree (which is just an operation).

3. 1. SELECT
3. 2. JOIN
    -> If the node is a JOIN node, then it should have a JoinAlgorithm attribute which is an enumeration of available join algorithms.

3. 3. INDEX_SCAN
3. 4. SORTING



"""

class NodeType(Enum):
    """
    Enumeration of possible node types in a query execution plan tree.
    Represents the different relational algebra operations and access methods.
    """
    SELECT = "SELECT"     
    PROJECT = "PROJECT"    
    JOIN = "JOIN"         
    INDEX_SCAN = "INDEX_SCAN"  
    SORTING = "SORTING"

class JoinAlgorithm(Enum):
    """
    Enumeration of available join algorithms.
    Each algorithm has different performance characteristics based on input relations.
    """
    NESTED_LOOP = "nested_loop"
    MERGE = "merge"
    HASH = "hash"

class JoinCondition:
    """
    Represents a join condition between two relations.
    Contains the attributes to be joined and the comparison operator.
    """
    def __init__(self, left_attr: str, right_attr: str, operator: str = "="):
        pass