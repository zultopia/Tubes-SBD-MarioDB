from query_plan.base import QueryNode
from query_plan.nodes.selection_node import SelectionNode,SelectionCondition
from query_plan.nodes.join_nodes import ConditionalJoinNode,NaturalJoinNode, JoinCondition
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

    '''
    RULE 4
    assumption: a ConditionalJoinNode which has no condition is a cartesian product
    kinda cheated on this one by making parent a parameter, sebaiknya ada atribut parent di base.py
    '''
    @staticmethod
    def combineJoinCondition(parent:QueryNode,node: QueryNode) -> None:
        if(node.hasOneChild())and(isinstance(node,SelectionNode)):
            if(isinstance(node.children,ConditionalJoinNode)):
                combinedNode = node.children
                newConditions = []
                for condition in node.conditions:
                    newCondition = JoinCondition(
                        condition.left_operand,
                        condition.right_operand,
                        condition.operator.value 
                    )
                    newConditions.append(newCondition) 
                for c in newCondition:
                    combinedNode.conditions.append(c)
                parent.children=combinedNode
            #idk how to get the attribute with the same name
            elif (isinstance(node.children,NaturalJoinNode)):
                pass


    '''
    RULE 5
    switches the children of natural and theta joins
    '''
    @staticmethod
    def switchChildrenJoin(node: QueryNode) -> None:
        if (isinstance(node,ConditionalJoinNode)or isinstance(node,NaturalJoinNode)):
            node.switchChildren()
        

