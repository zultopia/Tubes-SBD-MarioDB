from .base import QueryNode
from .optimizers.optimizer import QueryPlanOptimizer
from utils import Prototype
from .nodes.join_nodes import JoinNode, ConditionalJoinNode, NaturalJoinNode
from .nodes.sorting_node import SortingNode
from .nodes.project_node import ProjectNode
from .nodes.table_node import TableNode
from .nodes.selection_node import SelectionNode


class QueryPlan(Prototype):
    def __init__(self, root: QueryNode):
        self.root = root
    
    def optimize(self, optimizer: QueryPlanOptimizer):
        return optimizer.optimize(self)
    
    def execute(self):
        # Implementation here
        pass
    
    def print(self):
        """
        Prints a visual representation of the query plan tree structure.
        Each node is printed with its specific attributes and relationships.
        """
        def print_node(node: QueryNode, level: int = 0) -> None:
            indent = "    " * level
            prefix = f"{indent}└─ " if level > 0 else ""

            # Handle base node case
            if node is None:
                return

            # Start building the node string
            node_str = str(node)
            
            # Print the current node
            print(f"{prefix}{node_str}")
            
            # Handle child nodes based on node type
            if isinstance(node, (ProjectNode, SelectionNode, SortingNode)):
                if hasattr(node, 'child') and node.child:
                    print_node(node.child, level + 1)
                    
            elif isinstance(node, (JoinNode, ConditionalJoinNode, NaturalJoinNode)):
                if hasattr(node, 'children') and node.children:
                    if node.children.first:
                        print_node(node.children.first, level + 1)
                    if node.children.second:
                        print_node(node.children.second, level + 1)

        print("\nQuery Plan:")
        print_node(self.root)
    
