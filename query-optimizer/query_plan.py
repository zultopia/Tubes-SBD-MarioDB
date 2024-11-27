from enum import Enum
from typing import Dict, List, Union, Literal
from utils import Prototype, Pair
from abc import ABC, abstractmethod
from parse_tree import ParseTree


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
    SORTING = "SORTING"
    TABLE = "TABLE"

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
    left_attr: str
    right_attr: str
    operator: Literal['>', '>=', '<', '<=', '=']

    def __init__(self, left_attr: str, right_attr: str, 
                 operator: Literal['>', '>=', '<', '<=', '=']):
        self.left_attr = left_attr
        self.right_attr = right_attr
        self.operator = operator


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
    children: Union['QueryNode', Pair['QueryNode', 'QueryNode'], None]

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
    condition: List[str]

    def __init__(self, condition: List[str]):
        super().__init__(NodeType.PROJECT)
        self.condition = condition
        self.child = None
    
    def _calculate_operation_cost(self, statistics: Dict) -> float:
        """
        Calculate cost of selection operation based on input size and condition.
            
        Returns:
            Estimated cost of selection operation
        """
        pass

    def estimate_cost(self, statistics: Dict) -> float:
        pass

    def set_child(self, childr: QueryNode):
        self.child = childr



class JoinNode(QueryNode):
    """
    Represents a join operation in the query plan.
    Combines two relations based on specified conditions.
    
    Attributes:
        algorithm: Specific join algorithm to use
    """
    algorithm: JoinAlgorithm

    def __init__(self, algorithm: JoinAlgorithm):
        super().__init__(NodeType.JOIN)
        self.algorithm = algorithm
        self.children = None
    
    def _calculate_operation_cost(self, statistics: Dict) -> float:
        """
        Calculate cost of join operation based on chosen algorithm and input sizes.
    
            
        Returns:
            Estimated cost of join operation
        """
        pass

    def set_children(self, children: Pair[QueryNode, QueryNode]):
        self.children = children

class ConditionalJoinNode(JoinNode):
    """
    Represents a join operation in the query plan.
    Combines two relations based on specified conditions.
    
    Attributes:
        algorithm: Specific join algorithm to use
        conditions: List of join conditions
    """
    algorithm: JoinAlgorithm
    conditions: List[JoinCondition]
    children: None | Pair[QueryNode, QueryNode]

    def __init__(self, algorithm: JoinAlgorithm, conditions: List[JoinCondition]):
        super().__init__(NodeType.JOIN)
        self.algorithm = algorithm
        self.conditions = conditions
        self.children = None
    
    def _calculate_operation_cost(self, statistics: Dict) -> float:
        """
        Calculate cost of join operation based on chosen algorithm and input sizes.
    
            
        Returns:
            Estimated cost of join operation
        """
        pass

    def estimate_cost(self, statistics: Dict) -> float:
        pass

class NaturalJoinNode(JoinNode):
    """
    Represents a join operation in the query plan.
    Combines two relations based on specified conditions.
    
    Attributes:
        algorithm: Specific join algorithm to use
    """
    algorithm: JoinAlgorithm
    children: None | Pair[QueryNode, QueryNode]

    def __init__(self, algorithm: JoinAlgorithm):
        super().__init__(NodeType.JOIN)
        self.algorithm = algorithm
        self.children = None
    
    def _calculate_operation_cost(self, statistics: Dict) -> float:
        """
        Calculate cost of join operation based on chosen algorithm and input sizes.
    
            
        Returns:
            Estimated cost of join operation
        """
        pass

    def estimate_cost(self, statistics: Dict) -> float:
        pass






class SortingNode(QueryNode):
    """
    Represents a sorting operation in the query plan.
    Orders the result rows based on specified attributes.
    
    Attributes:
        attributes: List of attributes to sort by
    """
    attributes: List[str]
    child: QueryNode

    def __init__(self, attributes: List[str]):
        super().__init__(NodeType.SORTING)
        self.attributes = attributes
        self.children = None
    
    def _calculate_operation_cost(self, statistics: Dict) -> float:
        """
        Calculate cost of sorting operation based on input size and attributes.
            
        Returns:
            Estimated cost of sorting operation
        """
        pass

    def estimate_cost(self, statistics: Dict) -> float:
        pass

    def set_child(self, child: QueryNode):
        self.child = child

class TableNode(QueryNode):
    """
    Represents a table access operation in the query plan.
    Reads rows from a table or view.
    """
    table_name: str

    def __init__(self, table_name: str):
        super().__init__(NodeType.TABLE)
        self.table_name = table_name
        self.children = None
    
    def _calculate_operation_cost(self, statistics: Dict) -> float:
        """
        Calculate cost of table access operation based on table size and indexes.
            
        Returns:
            Estimated cost of table access operation
        """
        pass

    def estimate_cost(self, statistics: Dict) -> float:
        pass


class QueryPlanOptimizer(ABC):
    """
    An ABC for query plan optimization algorithms.
    """
    
    @abstractmethod
    def optimize(self, query: 'QueryPlan') -> 'QueryPlan':
        """
        Optimize the query plan tree to reduce execution cost.
        
        Args:
            query: Parsed query tree to optimize
        
        Returns:
            Optimized query plan tree
        """
        pass


class QueryPlan(Prototype):
    """
    Represents the entire query execution plan tree.
    Provides methods for optimizing and executing the plan.
    
    Attributes:
        root: Root node of the query plan tree
        children: List of child nodes
    """
    root: QueryNode

    def __init__(self, root: QueryNode):
        self.root = root
    
    def optimize(self, optimizer: QueryPlanOptimizer):
        optimized = optimizer.optimize(self)
        self = optimized
    
    def execute(self):
        """
        Execute the query plan and return the result.
        
        Returns:
            Result of the query execution
        """
        pass

    def print(self):
        """
        Print the query plan tree in a human-readable format.
        """
        def print_node(node: QueryNode, level: int = 0):
            indent = "    " * level
            node_str = f"{indent}└─ {node}"
            
            if isinstance(node, ProjectNode):
                node_str += f" ({', '.join(node.condition)})"
                print(node_str)
                if hasattr(node, 'child') and node.child:
                    print_node(node.child, level + 1)
            
            elif isinstance(node, (JoinNode, ConditionalJoinNode, NaturalJoinNode)):
                node_str += f" [{node.algorithm.value}]"
                if isinstance(node, ConditionalJoinNode) and hasattr(node, 'conditions'):
                    conditions = [f"{c.left_attr} {c.operator} {c.right_attr}" for c in node.conditions]
                    node_str += f" ON {' AND '.join(conditions)}"
                print(node_str)
                if hasattr(node, 'children') and node.children:
                    print_node(node.children.first, level + 1)
                    print_node(node.children.second, level + 1)
            
            elif isinstance(node, SortingNode):
                node_str += f" BY {', '.join(node.attributes)}"
                print(node_str)
                if hasattr(node, 'child') and node.child:
                    print_node(node.child, level + 1)
            
            elif isinstance(node, TableNode):
                node_str += f" [{node.table_name}]"
                print(node_str)
        
        print("\nQuery Plan:")
        print_node(self.root)

class BFOptimizer(QueryPlanOptimizer):
    def optimize(self, query: QueryPlan) -> QueryPlan:
        reachable_plans = self.__generate_possible_plans(query)
        best_plan = None
        for plan in reachable_plans:
            # Not yet implemented
            pass

        return best_plan
    
    def __generate_possible_plans(self, query: QueryPlan) -> List[QueryPlan]:
        """
        Generate all possible query plans that can be reached from the given query plan.
        
        Args:
            query: Query plan to generate possible plans from
        
        Returns:
            List of possible query plans
        """
        
        # Not yet implemented
        pass



def from_parse_tree(parse_tree: ParseTree) -> QueryPlan:
    pass

if __name__ == "__main__":
    employees = TableNode("employees")
    departments = TableNode("departments")
    salaries = TableNode("salaries")
    
    join1 = ConditionalJoinNode(
        JoinAlgorithm.HASH,
        [JoinCondition("department_id", "id", "=")]
    )
    join1.set_children(Pair(employees, departments))
    
    join2 = NaturalJoinNode(JoinAlgorithm.MERGE)
    join2.set_children(Pair(join1, salaries))
    
    sort = SortingNode(["salary"])
    sort.set_child(join2)
    
    project = ProjectNode(["name", "department_name", "salary"])
    project.set_child(sort)
    
    query_plan = QueryPlan(project)
    query_plan.print()


        