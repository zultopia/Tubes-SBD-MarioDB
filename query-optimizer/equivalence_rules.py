from query_plan.base import QueryNode
from query_plan.nodes.selection_node import SelectionNode, UnionSelectionNode
from query_plan.nodes.join_nodes import ConditionalJoinNode,NaturalJoinNode, JoinNode
from query_plan.nodes.table_node import TableNode
from query_plan.nodes.project_node import ProjectNode
from query_plan.nodes.sorting_node import SortingNode
from query_plan.enums import NodeType
from typing import List, Union
from query_plan.shared import Condition
from data import QOData
from query_plan.enums import JoinAlgorithm
from utils import Pair

def get_all_attributes_of_node(node: 'QueryNode') -> List[str]:
    """
    Recursively determine all attributes produced by the given node.
    """
    # Base cases
    if isinstance(node, TableNode):
        # Get all attributes from QOData for the table
        qodata = QOData.get_instance()
        return qodata.get_all_attributes(node.table_name)

    elif isinstance(node, ProjectNode):
        # The project node only retains the projected attributes
        # They must be a subset of the child's attributes
        return node.attributes.copy()

    elif isinstance(node, SelectionNode):
        # Selection doesn't change the set of attributes, just filters rows
        # So it produces the same attributes as its child
        if node.child:
            return get_all_attributes_of_node(node.child)
        else:
            return []

    elif isinstance(node, UnionSelectionNode):
        # A union of selection nodes. Ideally, each child should produce the same schema.
        # Here we take the union of all children's attributes for safety.
        all_attrs = set()
        for child in node.children:
            child_attrs = get_all_attributes_of_node(child)
            all_attrs.update(child_attrs)
        return list(all_attrs)

    elif isinstance(node, (JoinNode, ConditionalJoinNode, NaturalJoinNode)):
        # Join combines two sets of attributes from left and right child.
        if node.children:
            left_attrs = get_all_attributes_of_node(node.children.first)
            right_attrs = get_all_attributes_of_node(node.children.second)
            # Union the sets
            attr_set = set(left_attrs) | set(right_attrs)
            return list(attr_set)
        else:
            return []

    elif isinstance(node, SortingNode):
        # Sorting doesn't change the set of attributes
        if node.child:
            return get_all_attributes_of_node(node.child)
        else:
            return []

    else:
        # If you have other node types, handle them here.
        # By default, return empty if unknown node type
        return []

def attribute_belongs_to(node: 'QueryNode', attr: str) -> bool:
    """
    Determine if the given attribute 'attr' is produced by the given node.
    """
    attrs = get_all_attributes_of_node(node)
    return attr in attrs


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
    
    # @staticmethod
    # def collapse_projections(node: QueryNode) -> List[QueryNode]:
    #     """
    #     RULE 3: Only the last projection in a sequence is needed
    #     """
    #     if not isinstance(node, ProjectNode):
    #         return [node]
        
    #     # Find the deepest child node
    #     current = node
    #     while isinstance(current.child, ProjectNode):
    #         current = current.child
        
    #     if current == node:  # No projection sequence found
    #         return [node]
        
    #     # Create new node with original projection but connected to deepest child
    #     new_node = node.clone()
    #     new_node.set_child(current.child)
    #     return [new_node]

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
    @staticmethod
    def switchChildrenJoin(node: QueryNode) -> List[QueryNode]:
        # Check if the node is a ConditionalJoinNode or NaturalJoinNode
        if not isinstance(node, (ConditionalJoinNode, NaturalJoinNode)):
            return [node]  # Return the original node as-is if it's not a join node

        # Clone the original node to ensure no modifications to the original
        if isinstance(node, ConditionalJoinNode):
            new_node = ConditionalJoinNode(
                algorithm=node.algorithm,
                conditions=node.conditions
            )
        elif isinstance(node, NaturalJoinNode):
            new_node = NaturalJoinNode(algorithm=node.algorithm)
        # print("This is the type of node clone:",nodeClone)
        # Switch the children in the cloned node
        if node.children and isinstance(node.children,Pair):
            new_node.set_children(Pair(node.children.second.clone(),node.children.first.clone()))

        # Return the modified clone along with the original node
        return [new_node]
    
    
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
        def filter_conditions(conditions: List[Condition], left: QueryNode, right: QueryNode) -> List[Condition]:
            """
            Filter conditions to include only those relevant to the given children.
            """
            relevant_conditions = []
            for condition in conditions:
                if checkRelevance(condition,left) or checkRelevance(condition,right):
                    relevant_conditions.append(condition)
            return relevant_conditions

        def create_associative_node(parent: QueryNode, outer: QueryNode, inner: QueryNode, is_left: bool) -> QueryNode:
            """Semua logic sebenearnya di sini"""
            if hasattr(inner, 'children') and isinstance(inner.children, Pair):
                grandchild_left = inner.children.first.clone() if inner.children.first else None
                grandchild_right = inner.children.second.clone() if inner.children.second else None
                # takes the class of input, assign the condition if it has the attribute else bakal jadi natural join
                filteredConditions=[]
                if isinstance(inner, ConditionalJoinNode) and is_left:
                    filteredConditions = filter_conditions(inner.conditions,grandchild_right,outer.clone())
                    associated_inner = ConditionalJoinNode(
                        algorithm=inner.algorithm,
                        conditions=filteredConditions
                    )
                elif isinstance(inner, ConditionalJoinNode) and not is_left:
                    filteredConditions = filter_conditions(inner.conditions,outer.clone(),grandchild_left)
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
                            conditions=parent.conditions
                        )
                    elif isinstance(parent, NaturalJoinNode):
                        new_node = NaturalJoinNode(algorithm=parent.algorithm)
                    new_node.set_children(Pair(grandchild_left, associated_inner))
                else:
                    associated_inner.set_children(Pair(outer.clone(), grandchild_left))
                    if isinstance(parent, ConditionalJoinNode):
                        new_node = ConditionalJoinNode(
                            algorithm=parent.algorithm,
                            conditions=parent.conditions
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
        return ret or [node.clone()] # kalau ret itu kosong aka falsy, maka akan dikirim [node]
    
    @staticmethod
    def distributeSelection(node: QueryNode) -> List[QueryNode]:
        """
        RULE 7: Selection operation can be distributed.
        - Distributes selection conditions across joins where applicable.
        """

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
        # Return the transformed join node
        return [new_join]


    """
        Rule 8:
        A. 
        If a projection on top of a join references attributes from the left side (L1) and the right side (L2) of the join separately, you can split the projection into two projections—one on each side of the join—and push them down. Formally:

        L1,L2(E1 ⋈ E2) = (L1(E1)) ⋈ (L2(E2))
    """
    @staticmethod
    def push_projections_into_join(node: QueryNode) -> List[QueryNode]:
        """Pushes projections into join nodes where applicable, ensuring join condition attributes are preserved."""
        if not isinstance(node, ProjectNode):
            return [node]
        
        def get_join_condition_attributes(join_node: QueryNode) -> List[str]:
            """Extracts all attributes used in join conditions."""
            if isinstance(join_node, ConditionalJoinNode):
                attrs = []
                for condition in join_node.conditions:
                    # Assuming conditions have left_operand and right_operand in "table.attr" format
                    attrs.append(condition.left_operand.split(".")[1])
                    attrs.append(condition.right_operand.split(".")[1])
                return attrs
            elif isinstance(join_node, NaturalJoinNode):
                # Natural joins implicitly join on attributes with the same name
                # Assuming QOData can provide the common attributes
                left_attrs = get_all_attributes_of_node(join_node.children.first)
                right_attrs = get_all_attributes_of_node(join_node.children.second)
                return list(set(left_attrs).intersection(set(right_attrs)))
            return []
        
        def push_projections_on_join(project_node: ProjectNode, join_node: Union[ConditionalJoinNode, NaturalJoinNode]) -> List[QueryNode]:
            """Handles pushing projections into ConditionalJoinNode or NaturalJoinNode."""
            # Get attributes from the projection
            proj_attrs = set(project_node.attributes)
            
            # Get attributes from join conditions
            join_attrs = set(get_join_condition_attributes(join_node))
            
            # Combine both sets to ensure join conditions are preserved
            required_attrs = proj_attrs.union(join_attrs)
            
            left_child = join_node.children.first
            right_child = join_node.children.second

            # Determine which attributes belong to which child
            left_attrs = set(get_all_attributes_of_node(left_child))
            right_attrs = set(get_all_attributes_of_node(right_child))
            
            # Attributes to project on the left child
            L1 = [attr for attr in required_attrs if attr in left_attrs]
            # Attributes to project on the right child
            L2 = [attr for attr in required_attrs if attr in right_attrs]
            
            # Handle attributes that might belong to both children (e.g., for Natural Joins)
            # Ensuring they are included in both projections if necessary
            shared_attrs = set(L1).intersection(set(L2))
            for attr in shared_attrs:
                # Ensure the attribute is present in both projections
                if attr not in L1:
                    L1.append(attr)
                if attr not in L2:
                    L2.append(attr)
            
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
            new_join = join_node.clone()
            new_join.set_children(Pair(new_left, new_right))
            
            return [new_join]
        
        # Scenario A: Project -> Join
        if isinstance(node.child, JoinNode):
            join_node = node.child
            if isinstance(join_node, ConditionalJoinNode) or isinstance(join_node, NaturalJoinNode):
                return push_projections_on_join(node, join_node)
            else:
                return [node]
        
        # Scenario B: Project -> Selection -> Join
        if isinstance(node.child, SelectionNode) and isinstance(node.child.child, JoinNode):
            selection_node = node.child
            join_node = selection_node.child
            
            if isinstance(join_node, ConditionalJoinNode) or isinstance(join_node, NaturalJoinNode):
                # First, push the selection down if possible (not covered here)
                # Then, push the projection down
                # For simplicity, we'll assume selection has already been handled
                return push_projections_on_join(node, join_node)
            else:
                return [node]
        
        # If none of the above, return the node as-is
        return [node]


    
    """
    Aditional Rule
    give variation of join the same query plan with different join algorithm
    """

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
                    conditions=node.conditions
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
