from query_plan.base import QueryNode
from query_plan.nodes.selection_node import SelectionNode,SelectionCondition
from query_plan.nodes.join_nodes import ConditionalJoinNode,NaturalJoinNode, JoinCondition
from query_plan.nodes.table_node import TableNode
from query_plan.nodes.project_node import ProjectNode
from query_plan.enums import NodeType
from utils import Pair
from typing import List
import uuid

class EquivalenceRules:

    '''
    RULE 1
    If it doesnt produce anything, then it will return the original node (reference)
    Else, it will return a list of nodes that are different from the original node
    '''
    @staticmethod
    def deconstruct_conjunction(node: QueryNode) -> List[QueryNode]:
        if node.node_type != NodeType.SELECTION:
            return [node]

        selection_node = node
        conditions = selection_node.conditions
        if len(conditions) <= 1:
            # No decomposition possible if only one or zero conditions
            return [node]

        original_child = selection_node.child.clone() if selection_node.child else None

        all_variants = []

        # Variant 1: No decomposition
        no_split_node = SelectionNode(conditions)
        if original_child:
            no_split_node.set_child(original_child.clone())
        all_variants.append(no_split_node)

        # Variants 2...: All possible splits
        # For each split point i, split conditions into left and right groups
        for i in range(1, len(conditions)):
            left_conditions = conditions[:i]
            right_conditions = conditions[i:]

            left_node = SelectionNode(left_conditions)
            right_node = SelectionNode(right_conditions)

            # The right node gets the original child subtree
            if original_child:
                right_node.set_child(original_child.clone())

            # Chain them: left_node -> right_node
            left_node.set_child(right_node)
            all_variants.append(left_node)

        return all_variants



    
    @staticmethod
    def commute_selections(node: QueryNode) -> List[QueryNode]:
        """
        RULE 2: Selection operations are commutative
        If node is a SelectionNode and its child is also a SelectionNode, we can swap their conditions.
        """
        if not (isinstance(node, SelectionNode) and 
                node.child and 
                isinstance(node.child, SelectionNode)):
            return [node]

        # node: SELECT conditions1
        # node.child: SELECT conditions2
        # We want to commute them to:
        # new_parent: SELECT conditions2
        #    └─ new_child: SELECT conditions1
        #         └─ node.child.child (if any)

        # Extract conditions
        top_conditions = node.child.conditions
        bottom_conditions = node.conditions

        # Clone subtree of node.child.child if it exists
        subtree = node.child.child.clone() if node.child.child else None

        # Create new parent (top) selection node
        new_parent = SelectionNode(top_conditions)

        # Create new child (bottom) selection node
        new_child = SelectionNode(bottom_conditions)

        # Attach the subtree under new_child
        if subtree:
            new_child.set_child(subtree)

        # Attach new_child under new_parent
        new_parent.set_child(new_child)

        return [new_parent]
    
    @staticmethod
    def collapse_projections(node: QueryNode) -> List[QueryNode]:
        """
        RULE 3: Only the last projection in a sequence is needed
        """
        if not isinstance(node, ProjectNode):
            return [node]
        
        # Find the deepest child node
        current = node
        while isinstance(current.child, ProjectNode):
            current = current.child
        
        if current == node:  # No projection sequence found
            return [node]
        
        # Create new node with original projection but connected to deepest child
        new_node = ProjectNode(node.attributes)
        new_node.set_child(current.child)
        return [new_node]

    '''
    RULE 4
    assumption: a ConditionalJoinNode which has no condition is a cartesian product
    kinda cheated on this one by making parent a parameter, sebaiknya ada atribut parent di base.py
    '''
    @staticmethod
    def combineJoinCondition(parent:QueryNode,node: QueryNode) -> None:
        if(isinstance(node.children,QueryNode))and(isinstance(node,SelectionNode)):
            if(isinstance(node.children,ConditionalJoinNode)):
                combinedNode = node.children
                newConditions = []
                for condition in node.conditions:
                    newCondition = JoinCondition(
                        condition.left_operand,
                        condition.right_operand,
                        condition.operator
                    )
                    newConditions.append(newCondition) 
                combinedNode.conditions.extend(newConditions)
            if parent:
                parent.children = combinedNode
            else: #if in root
                node = None
            

    '''
    RULE 5
    switches the children of natural and theta joins
    '''
    @staticmethod
    def switchChildrenJoin(node: QueryNode) -> None:
        if (isinstance(node,ConditionalJoinNode)or isinstance(node,NaturalJoinNode)):
            node.switchChildren()
        

# test functions 

def test_combineJoinCondition():
    selection_condition = SelectionCondition("a.id", "b.id", "=")
    join_condition = JoinCondition("c", "d", "=")
    selection_node = SelectionNode([selection_condition])
    join_node = ConditionalJoinNode(conditions=[join_condition])
    selection_node.children = join_node
    parent_node = SelectionNode([])
    parent_node.children = selection_node
    EquivalenceRules.combineJoinCondition(parent_node, selection_node)
    assert isinstance(parent_node.children, ConditionalJoinNode)  # Should now point to the join node
    assert len(parent_node.children.conditions) == 2  # One condition added
    assert parent_node.children.conditions[1].left_attr == "a.id"
    assert parent_node.children.conditions[1].right_attr == "b.id"
    print("Success")


def test_switchChildrenJoin():
    child1 = TableNode("a")
    child2 = TableNode("b")
    join_condition = JoinCondition("c", "d", "=")
    join_node = ConditionalJoinNode(conditions=[join_condition])
    join_node.children = Pair(child1, child2)
    EquivalenceRules.switchChildrenJoin(join_node)
    assert join_node.children.first == child2  # Check if children are swapped
    assert join_node.children.second == child1
    print("Success")
