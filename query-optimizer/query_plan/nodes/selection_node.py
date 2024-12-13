# selection_node.py

from typing import List, Dict, Optional
from query_plan.base import QueryNode
from query_plan.enums import NodeType, Operator
from data import QOData 
from .constants import *
from query_plan.shared import Condition 
from utils import Pair

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

    def estimate_size(self, statistics: Dict, alias_dict):
        if not self.child:
            return
        self.child.estimate_size()

        self.attributes = self.child.attributes
        self.n = self.child.n
        for condition in self.conditions:
            

            if condition.operator == Operator.EQ:
                left_table_name = alias_dict[condition.left_table_alias]
                right_table_name = alias_dict[condition.right_table_alias]
                self.n *= min(1 / QOData().get_V(condition.left_attribute, left_table_name), 1 / QOData().get_V(condition.right_attribute, right_table_name)) # Menurut buku, aman diasumsikan bahwa distribusinya uniform
            if condition.operator in [Operator.LESS, Operator.LESS_EQ]:
                left_table_name = alias_dict[condition.left_table_alias]
                self.n *= (float(condition.right_operand) - QOData().get_min(condition.left_attribute, left_table_name)) / (QOData().get_max(condition.left_attribute, left_table_name) - QOData().get_min(condition.left_attribute, left_table_name))
            if condition.operator in [Operator.GREATER,Operator.GREATER_EQ]:
                left_table_name = alias_dict[condition.left_table_alias]
                self.n *= (QOData().get_max(condition.left_attribute, left_table_name) - float(condition.right_operand) ) / (QOData().get_max(condition.left_attribute, left_table_name) - QOData().get_min(condition.left_attribute, left_table_name))
        
        
        self.n = int(self.n)
        if self.n < 0:
            self.n = 0
        self.b = self.child.b


    def estimate_cost(self, statistics: Dict, alias_dict) -> float:
        self.estimate_size()

        previous_cost = self.child.estimate_cost()

        is_index = False
        for condition in self.conditions:
            left_table_name = alias_dict[condition.left_table_alias]
            right_table_name = alias_dict[condition.right_table_alias]
            if (QOData().get_index(condition.left_attribute, left_table_name) == 'btree' or QOData().get_index(condition.right_attribute, right_table_name) == 'btree') and condition.operator != Operator.NEQ:
                is_index = True
                break
        
        if not is_index:
            return previous_cost + t_S + self.child.b * t_T + self.b * t_T
        else:
            c = 3
            # TODO: find height of the tree
            return previous_cost + (c + 1) * (t_T + t_S) + self.b * t_T



    def _calculate_operation_cost(self, statistics: Dict) -> float:
        # Placeholder for actual cost calculation
        return 1.0

    def __str__(self) -> str:
        conditions_str = ' AND '.join([str(c) for c in self.conditions])
        return f"SELECT {conditions_str}"

    def get_node_attributes(self) -> List[Pair[str, str]]:
        """
        Returns the list of attributes from the child node.
        """
        if not self.child:
            raise ValueError("SelectionNode has no child.")
        return self.child.get_node_attributes()

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

    def estimate_size(self, statistics: Dict) -> float:
        pass

    def estimate_cost(self, statistics: Dict) -> float:
        pass

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        # Placeholder for actual cost calculation
        return 1.0

    def __str__(self) -> str:
        return "UNION"

    def get_node_attributes(self) -> List[Pair[str, str]]:
        """
        Returns the list of attributes from the first child node.
        Assumes all children have the same attributes.
        """
        if not self.children:
            raise ValueError("UnionSelectionNode has no children.")
        if self._cached_attributes is None:
            self._cached_attributes = self.children[0].get_node_attributes().copy()
        return self._cached_attributes