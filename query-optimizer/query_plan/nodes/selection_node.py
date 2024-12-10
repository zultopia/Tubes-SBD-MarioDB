from typing import List, Dict
from ..base import QueryNode
from ..enums import NodeType
from ..shared import Condition



class SelectionNode(QueryNode):
    def __init__(self, conditions: List[Condition]):
        super().__init__(NodeType.SELECTION)
        self.conditions = conditions
        self.child = None  # Single child node
        self.children = None  # Not used in SelectionNode

    def set_child(self, child: QueryNode):
        self.child = child

    def clone(self) -> 'SelectionNode':
        cloned_conditions = [Condition(c.left_operand, c.right_operand, c.operator) for c in self.conditions]
        cloned_node = SelectionNode(cloned_conditions)
        cloned_node.id = self.id

        if self.child:
            cloned_node.set_child(self.child.clone())
        return cloned_node

    def estimate_cost(self, statistics: Dict) -> float:
        return self._calculate_operation_cost(statistics)

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        return 1

    def __str__(self) -> str:
        return f"SELECT {', '.join([str(c) for c in self.conditions])}"

    def __eq__(self, other):
        if not super().__eq__(other):
            return False
        if len(self.conditions) != len(other.conditions):
            return False
        for c1, c2 in zip(self.conditions, other.conditions):
            if c1.left_operand != c2.left_operand or c1.operator != c2.operator or c1.right_operand != c2.right_operand:
                return False
        if (self.child is None) != (other.child is None):
            return False
        if self.child and other.child and self.child != other.child:
            return False
        return True

    def __hash__(self):
        conditions_tuple = tuple((c.left_operand, c.operator, c.right_operand) for c in self.conditions)
        child_hash = hash(self.child) if self.child else 0
        return hash((self.node_type, conditions_tuple, child_hash))
    

class UnionSelectionNode(QueryNode):
    def __init__(self, children: List[SelectionNode]):
        super().__init__(NodeType.UNION_SELECTION)
        self.children = children  

    def set_child_to_all(self, child: QueryNode):
        """Sets the given child node to all selection nodes in the union"""
        for selection_node in self.children:
            selection_node.set_child(child.clone())  

    def clone(self) -> 'UnionSelectionNode':
        cloned_children = [child.clone() for child in self.children]
        cloned_node = UnionSelectionNode(cloned_children)
        cloned_node.id = self.id

        return cloned_node

    def estimate_cost(self, statistics: Dict) -> float:
        return self._calculate_operation_cost(statistics)

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        # Placeholder implementation for union selection cost
        return 1

    def __str__(self) -> str:
        return f"UNION"

    def set_child_to_all(self, child: QueryNode):
        for selection_node in self.children:
            selection_node.set_child(child.clone())
    
    