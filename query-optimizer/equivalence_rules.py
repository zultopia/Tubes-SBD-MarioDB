from query_plan.base import QueryNode
from query_plan.nodes.selection_node import SelectionNode
from typing import List

class EquivalenceRules:

    '''
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
        

