# selection_node.py

from typing import List, Dict, Optional
from query_plan.base import QueryNode
from query_plan.enums import NodeType
from data import QOData 
from query_plan.shared import Condition 

class SelectionNode(QueryNode):
    def __init__(self, conditions: List['Condition']):
        super().__init__(NodeType.SELECTION)
        self.conditions = sorted(conditions, key=lambda c: (c.operator != 'EQ', c.left_operand, c.right_operand))
        self.child = None  # Single child node

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
        # Placeholder for actual cost calculation
        return 1.0

    def __str__(self) -> str:
        conditions_str = ' AND '.join([str(c) for c in self.conditions])
        return f"SELECT {conditions_str}"

    def attributes(self) -> List[str]:
        """
        Returns the list of attributes from the child node.
        """
        if not self.child:
            raise ValueError("SelectionNode has no child.")
        return self.child.attributes()

class UnionSelectionNode(QueryNode):
    def __init__(self, children: List['SelectionNode']):
        super().__init__(NodeType.UNION_SELECTION)
        self.children = children  # List of SelectionNode instances
        self._cached_attributes: Optional[List[str]] = None  # Cache for attributes

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
        # Placeholder for actual cost calculation
        return 1.0

    def __str__(self) -> str:
        return "UNION"

    def attributes(self) -> List[str]:
        """
        Returns the list of attributes from the first child node.
        Assumes all children have the same attributes.
        """
        if not self.children:
            raise ValueError("UnionSelectionNode has no children.")
        if self._cached_attributes is None:
            self._cached_attributes = self.children[0].attributes().copy()
        return self._cached_attributes