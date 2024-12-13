import math
from typing import List, Literal, Dict

from data import BLOCK_SIZE, QOData
from ..base import QueryNode
from ..enums import NodeType, JoinAlgorithm
from utils import Pair
from ..shared import Condition
from ..enums import Operator
from .constants import *

class JoinNode(QueryNode):
    def __init__(self, algorithm: JoinAlgorithm = JoinAlgorithm.NESTED_LOOP):
        super().__init__(NodeType.JOIN)
        self.algorithm = algorithm
        self.children: Pair[QueryNode, QueryNode] = None  # Should be of type Pair[QueryNode, QueryNode]
        self.table_name = None

    def set_children(self, children: Pair[QueryNode, QueryNode]):
        self.children = children

    def clone(self) -> 'JoinNode':
        cloned_children = None
        if self.children:
            cloned_children = Pair(self.children.first.clone(), self.children.second.clone())
        cloned_node = JoinNode(self.algorithm)
        cloned_node.id = self.id
        cloned_node.set_children(cloned_children)
        return cloned_node

    # def estimate_size(self, statistics: Dict):
    #     pass

    # def estimate_cost(self, statistics: Dict) -> float:
    #     pass

    def __str__(self) -> str:
        return f"JOIN [{self.algorithm.value}]"





class ConditionalJoinNode(JoinNode):
    def __init__(self, algorithm: JoinAlgorithm = JoinAlgorithm.NESTED_LOOP, conditions: List[Condition] = None):
        super().__init__(algorithm)
        self.node_type = NodeType.JOIN

        # Sort the condition based on Operator.EQ first then the other, then sort by left operand and right operand

        conditions = sorted(conditions, key=lambda c: c.operator != Operator.EQ)
        conditions = sorted(conditions, key=lambda c: c.left_operand)
        conditions = sorted(conditions, key=lambda c: c.right_operand)



        self.conditions = conditions if conditions is not None else []

    def clone(self) -> 'ConditionalJoinNode':
        cloned_conditions = [Condition(c.left_operand, c.right_operand, c.operator) for c in self.conditions]
        cloned_children = None
        
        
        if self.children:
            cloned_children = Pair(self.children.first.clone(), self.children.second.clone())
        cloned_node = ConditionalJoinNode(self.algorithm, cloned_conditions)
        cloned_node.id = self.id

        cloned_node.set_children(cloned_children)

        return cloned_node

    def estimate_size(self, statistics: Dict, alias_dict):
        assert (isinstance(self.children, Pair[QueryNode, QueryNode]))
        
        left: QueryNode = self.children.first
        right: QueryNode = self.children.second
        left.estimate_size()
        right.estimate_size()

        
        left_attributes = left.attributes
        right_attributes = right.attributes
        
        self.attributes = left_attributes + right_attributes

        self.n = left.n * right.n
        for condition in self.conditions:
            left_table_name = alias_dict[condition.left_table_alias]
            right_table_name = alias_dict[condition.right_table_alias]

            if condition.operator == Operator.EQ:
                self.n *= min(1 / QOData().get_V(condition.left_attribute, left_table_name), 1 / QOData().get_V(condition.right_attribute, right_table_name)) # Menurut buku, aman diasumsikan bahwa distribusinya uniform
            if condition.operator in [Operator.LESS, Operator.LESS_EQ]:
                self.n *= (float(condition.right_operand) - QOData().get_min(condition.left_attribute, left_table_name)) / (QOData().get_max(condition.left_attribute, left_table_name) - QOData().get_min(condition.left_attribute, left_table_name))
            if condition.operator in [Operator.GREATER,Operator.GREATER_EQ]:
                self.n *= (QOData().get_max(condition.left_attribute, left_table_name) - float(condition.right_operand) ) / (QOData().get_max(condition.left_attribute, left_table_name) - QOData().get_min(condition.left_attribute, left_table_name))
        
        if self.n < 0:
            self.n = 0

        self.n = int(self.n)
        
        self.b = int(1 / (1 / left.b + 1 / right.b))



    def estimate_cost(self, statistics: Dict, alias_dict) -> float:
        self.estimate_size()

        left: QueryNode = self.children.first
        right: QueryNode = self.children.second

        previous_cost = left.estimate_cost() + right.estimate_cost()

        
        if self.algorithm == JoinAlgorithm.NESTED_LOOP:
            is_index = False
            for condition in self.conditions:
                left_table_name = alias_dict[condition.left_table_alias]
                right_table_name = alias_dict[condition.right_table_alias]
                if QOData().get_index(condition.left_attribute, left_table_name) == 'btree' or QOData().get_index(condition.right_attribute, right_table_name) == 'btree':
                    is_index = True
                    break
            
            if not is_index:
                return previous_cost + (left.b + right.b) * t_T + 2 * t_S
            else:
                # TODO: Height dari tree perlu dicari
                c = 3
                return previous_cost + left.b * (t_T + t_S) + left.n * c
        elif self.algorithm == JoinAlgorithm.HASH:
            is_index = False
            for condition in self.conditions:
                left_table_name = alias_dict[condition.left_table_alias]
                right_table_name = alias_dict[condition.right_table_alias]
                if QOData().get_index(condition.left_attribute, left_table_name) == 'hash' or QOData().get_index(condition.right_attribute, right_table_name) == 'hash':
                    is_index = True
                    break
            if not is_index:
                return Exception("The index does not exist")
            else:
                return previous_cost + (left.b + right.b) * t_T + 2 * t_S
        elif self.algorithm == JoinAlgorithm.MERGE:
            # No seeks required because b_b -> infinity implies seeks -> 0 
            return previous_cost + (left.b + right.b) * t_T
        


    def __str__(self) -> str:

        if not self.conditions:
            return f"JOIN [{self.algorithm.value}]"
        conditions_str = ', '.join([f"{c.left_operand} {c.operator.value} {c.right_operand}" for c in self.conditions])
        return f"JOIN [{self.algorithm.value}] ON {conditions_str}"


class NaturalJoinNode(JoinNode):
    def __init__(self, algorithm: JoinAlgorithm = JoinAlgorithm.NESTED_LOOP):
        super().__init__(algorithm)
        self.node_type = NodeType.JOIN

    def clone(self) -> 'NaturalJoinNode':
        cloned_children = None
        if self.children:

            cloned_children = Pair(self.children.first.clone(), self.children.second.clone())
        cloned_node = NaturalJoinNode(self.algorithm)
        cloned_node.id = self.id

        cloned_node.set_children(cloned_children)
        return cloned_node

    def estimate_size(self, statistics: Dict, alias_dict):
        assert (isinstance(self.children, Pair[QueryNode, QueryNode]))
        
        left: QueryNode = self.children.first
        right: QueryNode = self.children.second
        left.estimate_size()
        right.estimate_size()

        left_attributes = left.attributes
        right_attributes = right.attributes
        
    
        self.attributes = []
        common = []
        for i in left_attributes:
            left_attribute, left_alias = i
            for j in right_attributes: 
                right_attribute, right_alias = j
                if left_attribute == right_attribute and not any(left_attribute == attr for attr, _ in common):
                    common.append((left_attribute, left_alias))
                    self.attributes.append((left_attribute, left_alias))
    

        for i in left_attributes:
            left_attribute, left_alias = i
            if not any(left_attribute == attr for attr, _ in common):
                self.attributes.append((left_attribute, left_alias))
        
        for i in right_attributes:
            right_attribute, right_alias = i
            if not any(right_attribute == attr for attr, _ in common):
                self.attributes.append((right_attribute, right_alias))

        self.n = left.n * right.n
        for condition in self.conditions:
            left_table_name = alias_dict[condition.left_table_alias]
            right_table_name = alias_dict[condition.right_table_alias]

            if condition.operator == Operator.EQ:
                self.n *= min(1 / QOData().get_V(condition.left_attribute, left_table_name), 1 / QOData().get_V(condition.right_attribute, right_table_name)) # Menurut buku, aman diasumsikan bahwa distribusinya uniform
            if condition.operator in [Operator.LESS, Operator.LESS_EQ]:
                self.n *= (float(condition.right_operand) - QOData().get_min(condition.left_attribute, left_table_name)) / (QOData().get_max(condition.left_attribute, left_table_name) - QOData().get_min(condition.left_attribute, left_table_name))
            if condition.operator in [Operator.GREATER,Operator.GREATER_EQ]:
                self.n *= (QOData().get_max(condition.left_attribute, left_table_name) - float(condition.right_operand) ) / (QOData().get_max(condition.left_attribute, left_table_name) - QOData().get_min(condition.left_attribute, left_table_name))
        
        self.n = int(self.n)

        if self.n < 0:
            self.n = 0

        self.b = int(1 / (1 / left.b + 1 / right.b))

    def estimate_cost(self, statistics: Dict, alias_dict) -> float:
        self.estimate_size()

        left: QueryNode = self.children.first
        right: QueryNode = self.children.second

        previous_cost = left.estimate_cost() + right.estimate_cost()

        left_attributes = left.attributes
        right_attributes = right.attributes

        
        if self.algorithm == JoinAlgorithm.NESTED_LOOP:
            is_index = False
            common = []
            for i in left_attributes:
                left_attribute, left_alias = i
                for j in right_attributes: 
                    right_attribute, right_alias = j
                    if left_attribute == right_attribute and not any(left_attribute == attr for attr, _ in common):
                        common.append((left_attribute, left_alias))
            for attr, alias in common:
                table = alias_dict[alias]
                if QOData().get_index(attr, table) == 'btree':
                    is_index = True
                    break

            if not is_index:
                return previous_cost + (left.b + right.b) * t_T + 2 * t_S
            else:
                # Todo: Height dari tree perlu dicari
                c = 3
                return previous_cost + left.b * (t_T + t_S) + left.n * c
        elif self.algorithm == JoinAlgorithm.HASH:
            is_index = False
            common = []
            for i in left_attributes:
                left_attribute, left_alias = i
                for j in right_attributes: 
                    right_attribute, right_alias = j
                    if left_attribute == right_attribute and not any(left_attribute == attr for attr, _ in common):
                        common.append((left_attribute, left_alias))
            for attr, alias in common:
                table = alias_dict[alias]
                if QOData().get_index(attr, table) == 'hash':
                    is_index = True
                    break
            if not is_index:
                return Exception("The index does not exist")
            else:
                return previous_cost + (left.b + right.b) * t_T + 2 * t_S
        elif self.algorithm == JoinAlgorithm.MERGE:
            # No seeks required because b_b -> infinity implies seeks -> 0 
            return previous_cost + (left.b + right.b) * t_T
    

    def __str__(self) -> str:
        return f"NATURAL JOIN [{self.algorithm.value}]"


