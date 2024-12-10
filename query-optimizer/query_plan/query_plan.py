# query_plan.py
from typing import Optional
from .base import QueryNode
from .nodes.project_node import ProjectNode
from .nodes.selection_node import SelectionNode, UnionSelectionNode
from .nodes.table_node import TableNode
from .nodes.join_nodes import JoinNode, ConditionalJoinNode, NaturalJoinNode
from .nodes.sorting_node import SortingNode
from utils import Pair, Prototype


class QueryPlan(Prototype):
    def __init__(self, root: QueryNode):
        self.root = root

    def optimize(self, optimizer: 'QueryPlanOptimizer'):
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
            elif isinstance(node, (JoinNode, ConditionalJoinNode, NaturalJoinNode)) and isinstance(node.children, Pair):
                print_node(node.children.first, level + 1)
                print_node(node.children.second, level + 1)
            elif isinstance(node, SortingNode) and node.child:
                print_node(node.child, level + 1)
    
        print("\nQuery Plan:")
        print_node(self.root)

    def clone(self) -> 'QueryPlan':
        cloned_root = self.root.clone()
        cloned_plan = QueryPlan(cloned_root)
        return cloned_plan

    def serialize(self) -> str:
            """
            Serializes the entire query plan into a unique string representation.
            """
            def serialize_node(node: QueryNode) -> str:
                if isinstance(node, ProjectNode):
                    child_str = serialize_node(node.child) if node.child else ''
                    return f"PROJECT[{','.join(node.attributes)}]->{child_str}"
                elif isinstance(node, UnionSelectionNode):
                    children_str = ','.join([serialize_node(child) for child in node.children])
                    return f"UNION[{children_str}]"
                elif isinstance(node, SelectionNode):
                    conditions_str = ','.join([str(c) for c in node.conditions])
                    child_str = serialize_node(node.child) if node.child else ''
                    return f"SELECT[{conditions_str}]->{child_str}"
                elif isinstance(node, JoinNode):
                    children_str = f"{serialize_node(node.children.first)}|{serialize_node(node.children.second)}"
                    return f"{node.__class__.__name__}[{node.algorithm.value}]->{children_str}"
                elif isinstance(node, SortingNode):
                    order = "ASC" if node.ascending else "DESC"
                    child_str = serialize_node(node.child) if node.child else ''
                    return f"SORT[{','.join(node.attributes)} {order}]->{child_str}"
                elif isinstance(node, TableNode):
                    return f"TABLE[{node.table_name} AS {node.alias}]"
                else:
                    return "UNKNOWN"

            return serialize_node(self.root)

    def __eq__(self, other):
        if not isinstance(other, QueryPlan):
            return False
        return self.serialize() == other.serialize()

    def __hash__(self):
        return hash(self.serialize()) 
