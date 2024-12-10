from query_plan.base import QueryNode
from query_plan.nodes.selection_node import SelectionNode
from query_plan.nodes.join_nodes import ConditionalJoinNode,NaturalJoinNode
from query_plan.nodes.table_node import TableNode
from query_plan.nodes.project_node import ProjectNode
from query_plan.enums import NodeType
from utils import Pair
from typing import List
from query_plan.shared import Condition

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
            # No decomposition if only one or zero conditions
            return [node]

        original_child = selection_node.child.clone() if selection_node.child else None

        all_variants = []

        # Variant 1: No decomposition
        no_split_node = SelectionNode(conditions)
        if original_child:
            no_split_node.set_child(original_child.clone())
        all_variants.append(no_split_node)

        # Now, for each condition, make it the left node, and the rest as the right node
        for i in range(len(conditions)):
            left_condition = [conditions[i]]
            right_conditions = conditions[:i] + conditions[i+1:]

            left_node = SelectionNode(left_condition)
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

    """
    RULE 4: Combine join conditions into ConditionalJoinNode.
    """
    @staticmethod
    def combineJoinCondition(node: QueryNode) -> List[QueryNode]:
        nodeClone = node.clone()
        if not isinstance(nodeClone, SelectionNode) or not isinstance(nodeClone.child, ConditionalJoinNode):
            return [nodeClone]
        combinedNode = nodeClone.child
        newConditions = [
            Condition(
                condition.left_operand,
                condition.right_operand,
                condition.operator
            )
            for condition in nodeClone.conditions
        ]
        combinedNode.conditions.extend(newConditions)
        return [combinedNode]

    '''
    RULE 5
    switches the children of natural and theta joins
    '''
    #revised
    def switchChildrenJoin(node: QueryNode) -> List[QueryNode]:
        if not isinstance(node, (ConditionalJoinNode, NaturalJoinNode)):
            return [node]
        nodeClone = node.clone()
        temp = nodeClone.children.first
        nodeClone.children.first = nodeClone.children.second
        nodeClone.children.second = temp
        return [nodeClone, node]
        

# test functions 

def test_combineJoinCondition():
    selection_condition = Condition("a.id", "b.id", "=")
    join_condition = Condition("c", "d", "=")
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
    join_condition = Condition("c", "d", "=")
    join_node = ConditionalJoinNode(conditions=[join_condition])
    join_node.children = Pair(child1, child2)
    EquivalenceRules.switchChildrenJoin(join_node)
    assert join_node.children.first == child2  # Check if children are swapped
    assert join_node.children.second == child1
    print("Success")
