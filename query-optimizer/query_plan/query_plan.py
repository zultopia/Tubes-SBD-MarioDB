from .base import QueryNode
from .optimizers.optimizer import QueryPlanOptimizer
from utils import Prototype
from .nodes.join_nodes import JoinNode, ConditionalJoinNode, NaturalJoinNode
from .nodes.sorting_node import SortingNode
from .nodes.project_node import ProjectNode
from .nodes.table_node import TableNode


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
    
