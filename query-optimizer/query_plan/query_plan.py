from collections import deque
from .base import QueryNode
from .optimizers.optimizer import QueryPlanOptimizer
from utils import Pair, Prototype
from .nodes.join_nodes import JoinNode, ConditionalJoinNode, NaturalJoinNode, JoinCondition
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

#     def apply_rules_and_print(self):
#         """
#         Traverses the query plan using BFS, applies Rule 4 and Rule 5 where applicable,
#         and prints the tree if a rule is successfully applied.
#         """
#         queue = deque([(None, self.root)])  # (parent, current_node)

#         while queue:
#             parent, node = queue.popleft()

#             # Apply Rule 4
#             if isinstance(node, SelectionNode) and node.hasOneChild() and isinstance(node.children, ConditionalJoinNode):
#                 print("\nApplying Rule 4:")
#                 combineJoinCondition(parent, node)
#                 self.print()

#             # Apply Rule 5
#             if isinstance(node, (ConditionalJoinNode, NaturalJoinNode)):
#                 print("\nApplying Rule 5:")
#                 switchChildrenJoin(node)
#                 self.print()

#             # Add children to the queue for BFS
#             if isinstance(node.children, QueryNode):
#                 queue.append((node, node.children))
#             elif isinstance(node.children, Pair):
#                 queue.append((node, node.children.first))
#                 queue.append((node, node.children.second))
#             elif isinstance(node.children, list):
#                 for child in node.children:
#                     queue.append((node, child))

# def combineJoinCondition(parent:QueryNode,node: QueryNode) -> None:
#     if(node.hasOneChild())and(isinstance(node,SelectionNode)):
#         if(isinstance(node.children,ConditionalJoinNode)):
#             combinedNode = node.children
#             newConditions = []
#             for condition in node.conditions:
#                 newCondition = JoinCondition(
#                     condition.left_operand,
#                     condition.right_operand,
#                     condition.operator.value 
#                 )
#                 newConditions.append(newCondition) 
#             combinedNode.conditions.extend(newConditions)
#             parent.children=combinedNode

# def switchChildrenJoin(node: QueryNode) -> None:
#     if (isinstance(node,ConditionalJoinNode)or isinstance(node,NaturalJoinNode)):
#         node.switchChildren()