from query_plan.base import QueryNode
from query_plan.nodes.selection_node import SelectionNode
from query_plan.nodes.join_nodes import ConditionalJoinNode,NaturalJoinNode
from query_plan.nodes.table_node import TableNode
from query_plan.nodes.project_node import ProjectNode
from query_plan.enums import NodeType
from utils import Pair
from typing import List
from query_plan.shared import Condition
from data import QOData
from query_plan.enums import JoinAlgorithm

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
        new_node = node.clone()
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
    
    
    """
    RULE 6: Associativity of Natural and Theta Joins
    - Natural joins are associative: R ⋈ S ⋈ T = (R ⋈ S) ⋈ T = R ⋈ (S ⋈ T)
    - Theta joins are associative when conditions are preserved
    """ 
    @staticmethod
    def associativeJoins(node: QueryNode) -> List[QueryNode]:
        if not isinstance(node, (NaturalJoinNode, ConditionalJoinNode)):
            return [node]
        if not isinstance(node.children, Pair) or not node.children.first or not node.children.second:
            return [node]

        left_child = node.children.first
        right_child = node.children.second
        ret = []

        def create_associative_node(parent: QueryNode, outer: QueryNode, inner: QueryNode, is_left: bool) -> QueryNode:
            """Semua logic sebenearnya di sini"""
            if hasattr(inner, 'children') and isinstance(inner.children, Pair):
                grandchild_left = inner.children.first.clone() if inner.children.first else None
                grandchild_right = inner.children.second.clone() if inner.children.second else None
                # takes the class of input, assign the condition if it has the attribute else bakal jadi natural join
                associated_inner = parent.__class__(inner.conditions if hasattr(inner, 'conditions') else None)
                if is_left:
                    associated_inner.set_children(grandchild_right, outer.clone())
                    new_node = parent.__class__(parent.conditions if hasattr(parent, 'conditions') else None)
                    new_node.set_children(grandchild_left, associated_inner)
                else:
                    associated_inner.set_children(outer.clone(), grandchild_left)
                    new_node = parent.__class__(parent.conditions if hasattr(parent, 'conditions') else None)
                    new_node.set_children(associated_inner, grandchild_right)
                return new_node
            return parent.clone()  

        # Handle (left ⋈ middle) ⋈ right
        if isinstance(left_child, (NaturalJoinNode, ConditionalJoinNode)):
            new_node = create_associative_node(node.clone(), right_child.clone(), left_child.clone(), is_left=True)
            ret.append(new_node)

        # Handle left ⋈ (middle ⋈ right)
        if isinstance(right_child, (NaturalJoinNode, ConditionalJoinNode)):
            new_node = create_associative_node(node.clone(), left_child.clone(), right_child.clone(), is_left=False)
            ret.append(new_node)

        # Return the transformations or the original node if no transformation was applied
        return ret or [node.clone()] # kalau ret itu kosong aka falsy, maka akan dikirim [node]
    
    @staticmethod
    def distributeSelection(node: QueryNode) -> List[QueryNode]:
        """
        RULE 7: Selection operation can be distributed.
        - Distributes selection conditions across joins where applicable.
        """
        def checkRelevance(c: Condition, _node: QueryNode) -> bool:
            leftOperand = c.left_operand
            attr = leftOperand
            table = ''

            # Split the operand into table and attribute if applicable
            if '.' in leftOperand:
                table, attr = leftOperand.split(".")

            # Check relevance for ProjectNode
            if isinstance(_node, ProjectNode):
                for attribute in _node.attributes:
                    if attr == attribute:
                        return True

            # Check relevance for TableNode
            elif isinstance(_node, TableNode):
                tempData = QOData()
                actualTable = tempData.data.get(_node.table_name)
                if actualTable:
                    attributes = actualTable["attributes"]
                    if attr in attributes:
                        return True

            # Check relevance for nodes with children (e.g., JoinNode, ConditionalJoinNode, etc.)
            elif hasattr(_node, 'children') and isinstance(_node.children, Pair):
                # Recursively check both children in the Pair
                if checkRelevance(c, _node.children.first) or checkRelevance(c, _node.children.second):
                    return True

            # Check relevance for a single child node
            elif hasattr(_node, 'child') and _node.child:
                return checkRelevance(c, _node.child)

            # If no match is found in the subtree, return False
            return False

        if not isinstance(node, SelectionNode) or not isinstance(node.child, (ConditionalJoinNode, NaturalJoinNode)):
            return [node]

        join_node = node.child
        if not isinstance(join_node.children, Pair) or not join_node.children.first or not join_node.children.second:
            return [node]
        left_child = join_node.children.first
        right_child = join_node.children.second

        # Split conditions based on relevance to left or right child
        left_conditions = []
        right_conditions = []
        for condition in node.conditions:
            if checkRelevance(condition,left_child):
                
                left_conditions.append(condition)
            elif checkRelevance(condition,right_child):
                # Check if condition applies to right_child
                right_conditions.append(condition)

        # If no conditions can be distributed, return the original node
        if not left_conditions and not right_conditions:
            return [node]
        # Create new selection nodes for left and right children if applicable
        if left_conditions:
            left_selection = SelectionNode(left_conditions)
            left_selection.set_child(left_child.clone())
        else:
            left_selection = left_child.clone()  # If no conditions apply, just clone the child
        if right_conditions:
            right_selection = SelectionNode(right_conditions)
            right_selection.set_child(right_child.clone())
        else:
            right_selection = right_child.clone()  # If no conditions apply, just clone the child
        # Create a new join node with the modified children
        if isinstance(join_node, ConditionalJoinNode):
            new_join = ConditionalJoinNode(
                algorithm=join_node.algorithm,
                conditions=join_node.conditions if hasattr(join_node, 'conditions') else None
            )
        elif isinstance(join_node, NaturalJoinNode):
            new_join = NaturalJoinNode(
                algorithm=join_node.algorithm
            )
        new_join.set_children(Pair(left_selection, right_selection))
        #new_join.algorithm = JoinAlgorithm.NESTED_LOOP
        # Return the transformed join node
        return [new_join]

        

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
