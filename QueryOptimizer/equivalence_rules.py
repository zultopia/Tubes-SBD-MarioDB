from QueryOptimizer.query_plan.base import QueryNode
from QueryOptimizer.query_plan.nodes.selection_node import SelectionNode, UnionSelectionNode
from QueryOptimizer.query_plan.nodes.join_nodes import ConditionalJoinNode,NaturalJoinNode, JoinNode
from QueryOptimizer.query_plan.nodes.table_node import TableNode
from QueryOptimizer.query_plan.nodes.project_node import ProjectNode
from QueryOptimizer.query_plan.nodes.sorting_node import SortingNode
from QueryOptimizer.query_plan.enums import NodeType
from typing import List, Union
from QueryOptimizer.query_plan.shared import Condition
from QueryOptimizer.data import QOData
from QueryOptimizer.query_plan.enums import JoinAlgorithm
from QueryOptimizer.utils import Pair
from copy import deepcopy


def get_all_attributes_of_node(node: 'QueryNode') -> List[str]:
    """
    Retrieve all attributes produced by the given node using its attributes() method.
    """
    if node is None:
        return []
    
    return node.get_node_attributes()

def attribute_belongs_to(node: 'QueryNode', attr: str) -> bool:
    if attr == '*':
        return True  # '*' is universally applicable
    """
    Determine if the given attribute 'attr' is produced by the given node.
    """
    attrs = get_all_attributes_of_node(node)
    return attr in attrs



class EquivalenceRules:

    '''
    RULE 1
    If the node doesn't produce anything, return the original node (reference).
    Else, return a list of nodes that are different from the original node.
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

        # Variants with each condition separated
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

    '''
    RULE 2: Selection operations are commutative
    If node is a SelectionNode and its child is also a SelectionNode, we can swap their conditions.
    '''
    @staticmethod
    def commute_selections(node: QueryNode) -> List[QueryNode]:
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

    '''
    RULE 4: Combine join conditions into ConditionalJoinNode.
    '''
    @staticmethod
    def combineJoinCondition(node: QueryNode) -> List[QueryNode]:
        if not (isinstance(node, SelectionNode) and isinstance(node.child, (ConditionalJoinNode, NaturalJoinNode))):
            return [node.clone()]

        selection_node = node
        join_node = selection_node.child

        if isinstance(join_node, ConditionalJoinNode):
            # Combine conditions
            combined_conditions = join_node.conditions + selection_node.conditions
            combined_node = ConditionalJoinNode(algorithm=join_node.algorithm, conditions=combined_conditions)
            combined_node.set_children(join_node.children.clone())
            return [combined_node]

        elif isinstance(join_node, NaturalJoinNode):
            # For NaturalJoinNode, selection conditions are not directly part of the join
            # They should remain as separate selection nodes
            return [node.clone()]

        return [node.clone()]

    '''
    RULE 5
    Switches the children of natural and theta joins
    '''
    @staticmethod
    def switchChildrenJoin(node: QueryNode) -> List[QueryNode]:
        # Check if the node is a ConditionalJoinNode or NaturalJoinNode
        if not isinstance(node, (ConditionalJoinNode, NaturalJoinNode)):
            return [node]  # Return the original node as-is if it's not a join node

        # Clone the original node to ensure no modifications to the original
        if isinstance(node, ConditionalJoinNode):
            new_node = ConditionalJoinNode(
                algorithm=node.algorithm,
                conditions=node.conditions.copy()
            )
        elif isinstance(node, NaturalJoinNode):
            new_node = NaturalJoinNode(algorithm=node.algorithm)

        # Switch the children in the cloned node
        if node.children and isinstance(node.children, Pair):
            new_node.set_children(Pair(node.children.second.clone(), node.children.first.clone()))

        # Return the modified clone
        return [new_node]

    '''
    RULE 6: Associativity of Natural and Theta Joins
    - Natural joins are associative: R ⋈ S ⋈ T = (R ⋈ S) ⋈ T = R ⋈ (S ⋈ T)
    - Theta joins are associative when conditions are preserved
    '''
    @staticmethod
    def associativeJoins(node: QueryNode) -> List[QueryNode]:
        if not isinstance(node, (NaturalJoinNode, ConditionalJoinNode)):
            return [node]
        if not isinstance(node.children, Pair) or not node.children.first or not node.children.second:
            return [node]

        left_child = node.children.first
        right_child = node.children.second
        ret = []
        conditionLeft = []
        def filter_conditions(conditions: List[Condition], left: QueryNode, right: QueryNode,condLen:int) -> List[Condition]:
            """
            Filter conditions to include only those relevant to the given children.
            """
            relevant_conditions = []
            for condition in conditions:
                if checkRelevance(condition,left) or checkRelevance(condition,right):
                    relevant_conditions.append(condition)
                else:
                    if(len(conditionLeft)<condLen):
                        conditionLeft.append(condition)
            return relevant_conditions

        def create_associative_node(parent: QueryNode, outer: QueryNode, inner: QueryNode, is_left: bool) -> QueryNode:
            """Semua logic sebenearnya di sini"""
            if hasattr(inner, 'children') and isinstance(inner.children, Pair):
                grandchild_left = inner.children.first.clone() if inner.children.first else None
                grandchild_right = inner.children.second.clone() if inner.children.second else None
                # takes the class of input, assign the condition if it has the attribute else bakal jadi natural join
                filteredConditions=[]
                if isinstance(inner, ConditionalJoinNode)and isinstance(parent,ConditionalJoinNode) and is_left:
                    conditionLen = len(inner.conditions) + len(parent.conditions)
                    relevantConditions1 = filter_conditions(inner.conditions,grandchild_right,outer.clone(),conditionLen)
                    relevantConditions2 = filter_conditions(parent.conditions,grandchild_right,outer.clone(),conditionLen)
                    filteredConditions += relevantConditions1 + relevantConditions2
                    associated_inner = ConditionalJoinNode(
                        algorithm=inner.algorithm,
                        conditions=filteredConditions
                    )
                elif isinstance(inner, ConditionalJoinNode) and isinstance(parent,ConditionalJoinNode) and not is_left:
                    conditionLen = len(inner.conditions) + len(parent.conditions)
                    relevantConditions1 = filter_conditions(inner.conditions,outer.clone(),grandchild_left,conditionLen)
                    relevantConditions2 = filter_conditions(parent.conditions,outer.clone(),grandchild_left,conditionLen)
                    associated_inner = ConditionalJoinNode(
                        algorithm=inner.algorithm,
                        conditions=filteredConditions
                    )
                elif isinstance(inner, NaturalJoinNode):
                    associated_inner = NaturalJoinNode(algorithm=inner.algorithm)
                if is_left:
                    associated_inner.set_children(Pair(grandchild_right, outer.clone()))
                    if isinstance(parent, ConditionalJoinNode):
                        new_node = ConditionalJoinNode(
                            algorithm=parent.algorithm,
                            conditions=conditionLeft
                        )
                    elif isinstance(parent, NaturalJoinNode):
                        new_node = NaturalJoinNode(algorithm=parent.algorithm)
                    new_node.set_children(Pair(grandchild_left, associated_inner))
                else:
                    associated_inner.set_children(Pair(outer.clone(), grandchild_left))
                    if isinstance(parent, ConditionalJoinNode):
                        new_node = ConditionalJoinNode(
                            algorithm=parent.algorithm,
                            conditions=conditionLeft
                        )
                    elif isinstance(parent, NaturalJoinNode):
                        new_node = NaturalJoinNode(algorithm=parent.algorithm)
                    new_node.set_children(Pair(associated_inner, grandchild_right))
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
        return ret or [node.clone()]  # If ret is empty, return the original node

    '''
    RULE 7: Selection operation can be distributed.
    - Distributes selection conditions across joins where applicable.
    '''
    @staticmethod
    def distributeSelection(node: QueryNode) -> List[QueryNode]:
        if not isinstance(node, SelectionNode):
            return [node]

        if not isinstance(node.child, (ConditionalJoinNode, NaturalJoinNode)):
            return [node]

        join_node = node.child
        left_child = join_node.children.first
        right_child = join_node.children.second

        # Split conditions based on relevance to left or right child
        left_conditions = []
        right_conditions = []
        for condition in node.conditions:
            if checkRelevance(condition, left_child):
                left_conditions.append(condition)
            elif checkRelevance(condition, right_child):
                right_conditions.append(condition)
            else:
                # Conditions that involve both sides or are unrelated remain in the current selection
                pass

        # If no conditions can be distributed, return the original node
        if not left_conditions and not right_conditions:
            return [node.clone()]

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
                conditions=join_node.conditions.copy()
            )
        elif isinstance(join_node, NaturalJoinNode):
            new_join = NaturalJoinNode(
                algorithm=join_node.algorithm
            )
        new_join.set_children(Pair(left_selection, right_selection))

        return [new_join]

    '''
    RULE 8:
    A. 
    If a projection on top of a join references attributes from the left side (L1) and the right side (L2) of the join separately, 
    you can split the projection into two projections—one on each side of the join—and push them down. Formally:

    '''
    @staticmethod
    def push_projections_into_join(node: QueryNode) -> List[QueryNode]:
        """Pushes projections into join nodes where applicable, ensuring join condition attributes are preserved."""
        if not isinstance(node, ProjectNode):
            return [node]

        def get_join_condition_attributes(join_node: JoinNode) -> List[str]:
            """Extracts all attributes used in join conditions."""
            if isinstance(join_node, ConditionalJoinNode):
                attrs = []
                for condition in join_node.conditions:
                    # Preserve fully qualified attribute names
                    attrs.append(condition.left_operand)
                    attrs.append(condition.right_operand)
                return attrs
            elif isinstance(join_node, NaturalJoinNode):
                # Natural joins implicitly join on attributes with the same name
                left_attrs = set(join_node.children.first.get_node_attributes())
                right_attrs = set(join_node.children.second.get_node_attributes())
                common_attrs = left_attrs.intersection(right_attrs)
                return list(common_attrs)
            return []

        def push_projections_on_join(project_node: ProjectNode, join_node: JoinNode) -> QueryNode:
            """Handles pushing projections into ConditionalJoinNode or NaturalJoinNode."""
            # Get attributes from the projection
            proj_attrs = set(project_node.projected_attributes)

            # Get attributes from join conditions
            join_attrs = set(get_join_condition_attributes(join_node))

            # Combine both sets to ensure join conditions are preserved
            required_attrs = proj_attrs.union(join_attrs)

            left_child = join_node.children.first
            right_child = join_node.children.second

            # Determine which attributes belong to which child
            left_attrs = set(left_child.get_node_attributes())
            right_attrs = set(right_child.get_node_attributes())

            # Attributes to project on the left child
            L1 = [attr for attr in required_attrs if attr in left_attrs]
            # Attributes to project on the right child
            L2 = [attr for attr in required_attrs if attr in right_attrs]

            # Create new ProjectNodes for left and right children if necessary
            new_left = left_child.clone()
            if L1 and set(L1) != left_attrs:
                left_proj = ProjectNode(L1)
                left_proj.set_child(new_left)
                new_left = left_proj

            new_right = right_child.clone()
            if L2 and set(L2) != right_attrs:
                right_proj = ProjectNode(L2)
                right_proj.set_child(new_right)
                new_right = right_proj

            # Create a new join node with the potentially projected children
            if isinstance(join_node, ConditionalJoinNode):
                new_join = ConditionalJoinNode(
                    algorithm=join_node.algorithm,
                    conditions=deepcopy(join_node.conditions),
                )
                new_join.set_children(Pair(new_left, new_right))
            elif isinstance(join_node, NaturalJoinNode):
                new_join = NaturalJoinNode(
                    algorithm=join_node.algorithm,
                )
                new_join.set_children(Pair(new_left, new_right))
            else:
                raise TypeError("Unsupported join node type.")
            return new_join

        # Scenario A: Project -> Join
        if isinstance(node.child, JoinNode):

            transformed_join = push_projections_on_join(node, node.child)
            return [transformed_join]

        # Scenario B: Project -> Selection -> Join
        if isinstance(node.child, SelectionNode) and isinstance(node.child.child, JoinNode):

            selection_node = node.child
            join_node = selection_node.child
            
            # Push projections into join
            transformed_join = push_projections_on_join(node, join_node)
            
            # Clone the selection node and set its child to the transformed join
            new_selection = selection_node.clone()
            new_selection.set_child(transformed_join)
            
            return [new_selection]
    
        return [node]

    '''
    Additional Rule:
    Give variation of join the same query plan with different join algorithms.
    '''
    @staticmethod
    def joinAlgorithmVariation(node: QueryNode) -> List[QueryNode]:
        """
        Additional Rule: Generate variations of join nodes with different algorithms.
        """
        variations = []

        # Check if the node is a ConditionalJoinNode or NaturalJoinNode
        if not isinstance(node, (NaturalJoinNode, ConditionalJoinNode)):
            return [node]  # Return the original node as-is if it's not a join node

        # Iterate over all join algorithms
        for algorithm in JoinAlgorithm:
            
            # Create a new join node with the same type and conditions but different algorithm
            if isinstance(node, ConditionalJoinNode):
                new_node = ConditionalJoinNode(
                    algorithm=algorithm,
                    conditions=node.conditions.copy()
                )
            elif isinstance(node, NaturalJoinNode):
                new_node = NaturalJoinNode(algorithm=algorithm)

            # Clone children to maintain subtree structure
            if node.children:
                new_node.set_children(
                    Pair(
                        node.children.first.clone(),
                        node.children.second.clone()
                    )
                )

            # Add the new node to variations
            variations.append(new_node)

        return variations





#helper functions
def checkRelevance(c: Condition, _node: QueryNode) -> bool:
    leftOperand = c.left_operand
    attr = leftOperand

    # Split the operand into table and attribute if applicable
    if '.' in leftOperand:
        table, attr = leftOperand.split(".")

    # Check relevance for ProjectNode
    if isinstance(_node, ProjectNode):
        for attribute in _node.projected_attributes:
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
