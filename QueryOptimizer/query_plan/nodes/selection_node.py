# selection_node.py

from typing import List, Dict, Optional
from QueryOptimizer.query_plan.base import QueryNode
from QueryOptimizer.query_plan.enums import NodeType, Operator
from QueryOptimizer.data import QOData 
from QueryOptimizer.query_plan.nodes.constants import *
from QueryOptimizer.query_plan.shared import Condition 
from QueryOptimizer.utils import Pair

class SelectionNode(QueryNode):
    def __init__(self, conditions: List['Condition']):
        super().__init__(NodeType.SELECTION)
        self.conditions = sorted(conditions, key=lambda c: (c.operator != 'EQ', c.left_operand, c.right_operand))
        self.child = None  # Single child node
        self.children = None

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
        self.child.estimate_size(statistics, alias_dict)

        self.attributes = self.child.attributes
        self.n = self.child.n
        for condition in self.conditions:

            # Case 1: Attr OPERATOR LITERAL
            if condition.right_table_alias is None:
                if condition.operator == Operator.EQ:
                    left_table_name = alias_dict[condition.left_table_alias]
                    self.n *= 1 / QOData().get_V(condition.left_attribute, left_table_name) # Menurut buku, aman diasumsikan bahwa distribusinya uniform
                if condition.operator in [Operator.LESS, Operator.LESS_EQ]:
                    left_table_name = alias_dict[condition.left_table_alias]
                    self.n *= (float(condition.right_operand) - QOData().get_min(condition.left_attribute, left_table_name)) / (QOData().get_max(condition.left_attribute, left_table_name) - QOData().get_min(condition.left_attribute, left_table_name))
                if condition.operator in [Operator.GREATER,Operator.GREATER_EQ]:
                    left_table_name = alias_dict[condition.left_table_alias]
                    self.n *= (QOData().get_max(condition.left_attribute, left_table_name) - float(condition.right_operand) ) / (QOData().get_max(condition.left_attribute, left_table_name) - QOData().get_min(condition.left_attribute, left_table_name))
            # Case 2: Attr OPERATOR Attr
            else:
                if condition.operator == Operator.EQ:
                    left_table_name = alias_dict[condition.left_table_alias]
                    right_table_name = alias_dict[condition.right_table_alias]
                    self.n *= min(1 / QOData().get_V(condition.left_attribute, left_table_name), 1 / QOData().get_V(condition.right_attribute, right_table_name)) # Menurut buku, aman diasumsikan bahwa distribusinya uniform

        self.n = int(self.n)
        if self.n < 0:
            self.n = 0
        self.b = self.child.b


    def estimate_cost(self, statistics: Dict, alias_dict) -> float:
        self.estimate_size(statistics, alias_dict)

        previous_cost = self.child.estimate_cost(statistics, alias_dict)

        is_index = False
        for condition in self.conditions:

            left_table_name = alias_dict[condition.left_table_alias]
            # Attr OPERATOR Literal
            if condition.right_table_alias is None:
                if QOData().get_index(condition.left_attribute, left_table_name) == 'btree':
                    is_index = True
                    break
        if not is_index:
            return previous_cost + t_S + self.child.b * t_T + self.b * t_T
        else:
            c = 3
            # TODO: find height of the tree
            return previous_cost + (c + self.n) * (t_T + t_S) + self.b * t_T



    def _calculate_operation_cost(self, statistics: Dict) -> float:
        # Placeholder for actual cost calculation
        return 1.0

    def __str__(self) -> str:
        conditions_str = ' AND '.join([str(c) for c in self.conditions])
        return f"SELECT {conditions_str}"

    def get_node_attributes(self) -> List[str]:
        if not self.child:
            raise ValueError("SelectionNode has no child.")
            
        # Get attributes from child node (already in table.attr format)
        child_attrs = self.child.get_node_attributes()
        
        # For each condition with constant comparison, match its attribute with child attributes
        for condition in self.conditions:
            if condition.is_constant_comparison():
                # Find matching attribute from child
                unqualified_attr = condition.left_attribute
                matching_attrs = [attr for attr in child_attrs if attr.split('.')[-1] == unqualified_attr]
                
                if len(matching_attrs) == 1:
                    # Single match - use its table alias
                    table_alias = matching_attrs[0].split('.')[0]
                    condition.left_table_alias = table_alias
                    
                elif len(matching_attrs) == 2:
                    # Two matches - in case of natural join, use the left table's version
                    left_match = next(attr for attr in matching_attrs if attr.startswith('student.'))
                    if left_match:
                        condition.left_table_alias = 'student'
                    else:
                        raise ValueError(f"Ambiguous attribute '{unqualified_attr}' in condition - please qualify with table name")
                    
                elif len(matching_attrs) > 2:
                    raise ValueError(f"Too many matching attributes found for '{unqualified_attr}'")
                else:
                    raise ValueError(f"Attribute '{unqualified_attr}' not found in child node attributes")
            else:
                #  make both left and right
                left_unqualified_attr = condition.left_attribute
                right_unqualified_attr = condition.right_attribute

                left_matching_attrs = [attr for attr in child_attrs if attr.split('.')[-1] == left_unqualified_attr]
                right_matching_attrs = [attr for attr in child_attrs if attr.split('.')[-1] == right_unqualified_attr]

                if len(left_matching_attrs) == 1:
                    # Single match - use its table alias
                    table_alias = left_matching_attrs[0].split('.')[0]
                    condition.left_table_alias = table_alias
                else:
                    raise ValueError(f"Attribute '{left_unqualified_attr}' not found in child node attributes")
            
                if len(right_matching_attrs) == 1:
                    # Single match - use its table alias
                    table_alias = right_matching_attrs[0].split('.')[0]
                    condition.right_table_alias = table_alias
                else:
                    if condition.is_constant_comparison():
                        condition.right_table_alias = condition.left_table_alias
        
                
        
        return child_attrs
                


class UnionSelectionNode(QueryNode):
    def __init__(self, children: List['SelectionNode']):
        super().__init__(NodeType.UNION_SELECTION)
        self.children: List['SelectionNode'] = children  # List of SelectionNode instances
        self.child = None
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

    def estimate_size(self, statistics: Dict, alias_dict) -> float:
        assert (len(self.children) > 0)
        
        self.n = 0
        for selection_node in self.children:
            selection_node.estimate_size(statistics, alias_dict)
            self.n += selection_node.n
        
        self.b = self.children[0].b

        self.attributes = self.children[0].attributes



    def estimate_cost(self, statistics: Dict, alias_dict) -> float:
        # No IO cost
        self.estimate_size(statistics, alias_dict)
        previous_cost = 0
        for seletion_node in self.children:
            previous_cost += seletion_node.estimate_cost(statistics, alias_dict)

        return previous_cost + self.b * t_T

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