from query_plan.base import QueryNode
from query_plan.nodes.selection_node import SelectionNode,SelectionCondition
from query_plan.nodes.join_nodes import ConditionalJoinNode,NaturalJoinNode, JoinCondition
from query_plan.nodes.table_node import TableNode
from query_plan.nodes.project_node import ProjectNode
from query_plan.enums import NodeType
from utils import Pair
from typing import List

class EquivalenceRules:

    '''
    RULE 1
    If it doesnt produce anything, then it will return the original node (reference)
    Else, it will return a list of nodes that are different from the original node
    '''
    @staticmethod
    def deconstruct_conjunction(node: QueryNode) -> List[QueryNode]:
        if (not isinstance(node, SelectionNode)):
            return [node]
        
        if (len(node.conditions) == 1):
            return [node]
        
        ret = []
        for condition in node.conditions:
            parent = SelectionNode([condition])

            # conditions other than the current condition
            children_conditions = [c for c in node.conditions if c != condition] 

            # create a new node with the remaining conditions
            child = SelectionNode(children_conditions) 

            # set the child node to the parent node
            parent.set_child(child)

            # add the parent node to the return list
            ret.append(parent)

        return ret
    
    @staticmethod
    def commute_selections(node: QueryNode) -> List[QueryNode]:
        """
        RULE 2: Selection operations are commutative
        """
        if not isinstance(node, SelectionNode) or not node.child or not isinstance(node.child, SelectionNode):
            return [node]
        
        # Create new node with swapped conditions
        new_parent = SelectionNode(node.child.conditions)
        new_child = SelectionNode(node.conditions)
        
        # Preserve the original child's child
        if node.child.child:
            new_child.set_child(node.child.child)
        
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
