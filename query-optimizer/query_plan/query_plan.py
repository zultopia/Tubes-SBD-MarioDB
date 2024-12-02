from .base import QueryNode
from .optimizers.optimizer import QueryPlanOptimizer
from utils import Prototype
from .nodes.join_nodes import JoinNode, ConditionalJoinNode, NaturalJoinNode
from .nodes.sorting_node import SortingNode
from .nodes.project_node import ProjectNode
from .nodes.table_node import TableNode
from .nodes.selection_node import SelectionNode, UnionSelectionNode


class QueryPlan(Prototype):
    def __init__(self, root: QueryNode):
        self.root = root
    
    def optimize(self, optimizer: QueryPlanOptimizer):
        return optimizer.optimize(self)
    
    def execute(self):
        # Implementation here
        pass
    
    def print(self):
        def print_node(node: QueryNode, level: int = 0):
            indent = "    " * level
            print(f"{indent}└─ {node}")
                
            if isinstance(node, ProjectNode) and node.child:
                print_node(node.child, level + 1)
            elif isinstance(node, UnionSelectionNode):
                for child in node.children:
                    print_node(child, level + 1)
            elif isinstance(node, SelectionNode) and node.child:
                print_node(node.child, level + 1)
            elif isinstance(node, (JoinNode, ConditionalJoinNode, NaturalJoinNode)) and node.children:
                print_node(node.children.first, level + 1)
                print_node(node.children.second, level + 1)
            elif isinstance(node, SortingNode) and node.child:
                print_node(node.child, level + 1)
        
        print("\nQuery Plan:")
        print_node(self.root)
    
