from enum import Enum
from typing import Dict, List, Union
from utils import Prototype
from abc import ABC, abstractmethod
from utils import Pair
"""
Complete documentation of what this file is about

1. Query plan is a tree based data structure, right?
2. But what does each node represent?

3. Here, my plan is that a node represents a so called "NodeType" which is an enumeration of possible node types in a query execution plan tree (which is just an operation).

3. 1. PROJECT
3. 2. JOIN
    -> If the node is a JOIN node, then it should have a JoinAlgorithm attribute which is an enumeration of available join algorithms.

3. 3. SORTING



"""

class NodeType(Enum):
    """
    Enumeration of possible node types in a query execution plan tree.
    Represents the different relational algebra operations and access methods.
    """
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


class QueryNode(ABC):
    """
    Abstract base class for all nodes in the query plan tree.
    Provides the basic structure and interface for query plan operations.
    
    Attributes:
        node_type: Type of operation this node represents
        estimated_cost: Estimated cost of this operation
        estimated_rows: Estimated number of result rows
    """
    node_type: NodeType
    children: Union[QueryNode, Pair[QueryNode, QueryNode]]

    def __init__(self, node_type: NodeType):
        self.node_type = node_type
        pass
    
    @abstractmethod
    def estimate_cost(self, statistics: Dict) -> float:
        """
        Calculate total cost of this operation including its children.
        
        Args:
            statistics (temporary, cuz maybe we can gain it from storage directly?): Dictionary containing statistical information about relations
            
        Returns:
            Total estimated cost of this subtree
        """
        pass
    
    @abstractmethod
    def _calculate_operation_cost(self, statistics: Dict) -> float:
        """
        Calculate cost of just this operation (without children).
        Must be implemented by concrete classes.
        
        Args:
            statistics (temporary, cuz maybe we can gain it from storage directly?): Dictionary containing statistical information about relations
            
        Returns:
            Estimated cost of this specific operation
        """
        pass

    def __str__(self) -> str:
        return self.node_type.value

class ProjectNode(QueryNode):
    """
    Represents a selection operation in the query plan.
    Filters rows based on a condition.
    
    Attributes:
        condition: Selection predicate to be applied
    """
    def __init__(self, condition: str):
        pass
    
    def _calculate_operation_cost(self, statistics: Dict) -> float:
        """
        Calculate cost of selection operation based on input size and condition.
            
        Returns:
            Estimated cost of selection operation
        """
        pass

    def set_children(self, children: QueryNode):
        self.children = children



class JoinNode(QueryNode):
    """
    Represents a join operation in the query plan.
    Combines two relations based on specified conditions.
    
    Attributes:
        algorithm: Specific join algorithm to use
        conditions: List of join conditions
    """
    def __init__(self, algorithm: JoinAlgorithm, conditions: List[JoinCondition]):
        pass
    
    def _calculate_operation_cost(self, statistics: Dict) -> float:
        """
        Calculate cost of join operation based on chosen algorithm and input sizes.
    
            
        Returns:
            Estimated cost of join operation
        """
        pass

    def set_children(self, children: Pair[QueryNode, QueryNode]):
        self.children = children

class QueryPlan(Prototype):
    """
    Represents the entire query execution plan tree.
    Provides methods for optimizing and executing the plan.
    
    Attributes:
        root: Root node of the query plan tree
        children: List of child nodes
    """
    def __init__(self, root: QueryNode):
        pass
    
    def optimize(self):
        """
        Optimize the query plan tree to reduce execution cost.
        """
        pass
    
    def execute(self):
        """
        Execute the query plan and return the result.
        
        Returns:
            Result of the query execution
        """
        pass

    def print_query(self): 
        """
        Print the SQL query represented by this query plan.
        """
        pass