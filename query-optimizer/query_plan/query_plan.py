# query_plan.py
from typing import Dict, Optional
from .base import QueryNode
from .nodes.project_node import ProjectNode
from .nodes.selection_node import SelectionNode, UnionSelectionNode
from .nodes.table_node import TableNode
from .nodes.join_nodes import JoinNode, ConditionalJoinNode, NaturalJoinNode
from .nodes.sorting_node import SortingNode
from .nodes.update_node import UpdateNode
from utils import Pair, Prototype


class QueryPlan(Prototype):
    def __init__(self, root: QueryNode):
        self.alias_dict: Dict[str, str] = {} # Given the alias, returns the original table name.
                             # For non-table expressions (result of joins, selections, projections, etc), it is assumed that there is no alias.
         

        self.root = root

    def optimize(self, optimizer: 'QueryPlanOptimizer'):
        # Panggil bf di sini?
        pass
        

    def execute(self):
        # Implementation here
        pass

    def estimate_cost(self, statistics: Dict) -> float:
        return self.root.estimate_cost(statistics, self.alias_dict)
    

    def __repr__(self):
        def repr_node(node: QueryNode, level: int = 0) -> str:
            indent = "    " * level
            node_str = f"{indent}└─ {node}"
            result = [node_str]
            
            if isinstance(node, ProjectNode):
                if hasattr(node, 'child') and node.child:
                    result.append(repr_node(node.child, level + 1))
            
            elif isinstance(node, UnionSelectionNode):
                for child in node.children:
                    result.append(repr_node(child, level + 1))
            
            elif isinstance(node, SelectionNode):
                if hasattr(node, 'child') and node.child:
                    result.append(repr_node(node.child, level + 1))
            
            elif isinstance(node, UpdateNode):
                if hasattr(node, 'child') and node.child:
                    result.append(repr_node(node.child, level + 1))
            
            elif isinstance(node, (JoinNode, ConditionalJoinNode, NaturalJoinNode)):
                if hasattr(node, 'children') and node.children:
                    result.append(repr_node(node.children.first, level + 1))
                    result.append(repr_node(node.children.second, level + 1))
            
            elif isinstance(node, SortingNode):
                node_str += f" BY {', '.join(node.attributes)}"
                if hasattr(node, 'child') and node.child:
                    result.append(repr_node(node.child, level + 1))
            
            elif isinstance(node, TableNode):
                pass  # TableNode has no children
                
            return '\n'.join(result)

        return f"{repr_node(self.root)}\n"

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
