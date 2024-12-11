import math
from typing import List, Literal, Dict

from data import BLOCK_SIZE, QOData
from ..base import QueryNode
from ..enums import NodeType, JoinAlgorithm
from utils import Pair
from ..shared import Condition
from ..enums import Operator

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

    def estimate_cost(self, statistics: Dict) -> float:
        return self._calculate_operation_cost(statistics)

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        # Placeholder implementation
        return 1

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

    def estimate_size(self, statistics: Dict):
        assert (isinstance(self.children, Pair[QueryNode, QueryNode]))
        
        left: QueryNode = self.children.first
        right: QueryNode = self.children.second
        left.estimate_size()
        right.estimate_size()

        
        left_attributes = left.attributes
        right_attributes = right.attributes
        
        # Todo: Ganti ID Kl perlu (?)
        self.attributes = left_attributes + right_attributes # kl conditional join, tidak ada intersectionnya

        self.n = left.n * right.n
        for condition in self.conditions:
            # Belum handle kasus left & right operandnya ada dot
            # Belum handle kasus atribut tidak ada di tabelnya
            # Belum handle kasus attribute < attribute (kyknya ga perlu krn eksotik)
            # Belum handle kasus NEQ (kyknya ga perlu) asumsi aja negligable karena dia tidak spesifik

            if condition.operator == Operator.EQ:
                self.n *= min(1 / QOData().get_V(condition.left_attribute, condition.left_table_name), 1 / QOData().get_V(condition.right_attribute, condition.right_table_name)) # Menurut buku, aman diasumsikan bahwa distribusinya uniform
            if condition.operator in [Operator.LESS, Operator.LESS_EQ]:
                self.n *= (float(condition.right_operand) - QOData().get_min(condition.left_attribute, condition.left_table_name)) / (QOData().get_max(condition.left_attribute, condition.left_table_name) - QOData().get_min(condition.left_attribute, condition.left_table_name))
            if condition.operator in [Operator.GREATER,Operator.GREATER_EQ]:
                self.n *= (QOData().get_max(condition.left_attribute, condition.left_table_name) - float(condition.right_operand) ) / (QOData().get_max(condition.left_attribute, condition.left_table_name) - QOData().get_min(condition.left_attribute, condition.left_table_name))
        self.n = int(self.n)

        self.b = int(1 / (1 / left.b + 1 / right.b))


    def estimate_io(self, statistics: Dict):
        pass     

    def estimate_cost(self, statistics: Dict) -> float:
        # Lagi integrasi fungsi calculate_join_cost biar menggunakan estimate_size
        pass

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        # Placeholder implementation for conditional joins
        return 1

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

    def estimate_size(self, statistics: Dict):
        assert (isinstance(self.children, Pair[QueryNode, QueryNode]))
        
        left: QueryNode = self.children.first
        right: QueryNode = self.children.second
        left.estimate_size()
        right.estimate_size()

        left_attributes = left.attributes
        right_attributes = right.attributes
        
        
        # Belum handle kasus nama atribut sama tapi tipenya berbeda (seharusnya tidak perlu karena ribet)

        self.attributes = []
        common = []
        for i in left_attributes:
            for j in right_attributes:
                if i.first.split('.')[1] == j.first.split('.')[1] : # if they have the same name
                    common.append(i.first.split('.')[1])
                    self.attributes.append((self.id + '.' + i.first.split('.')[1], i.second))
    

        for i in left_attributes:
            if i.first.split('.')[1] not in common:
                self.attributes.append((self.id + '.' + i.first.split('.')[1], i.second))
        
        for i in right_attributes:
            if i.first.split('.')[1] not in common:
                self.attributes.append((self.id + '.' + i.first.split('.')[1], i.second))

        self.n = left.n * right.n
        for condition in self.conditions:
            # Belum handle kasus left & right operandnya ada dot
            # Belum handle kasus atribut tidak ada di tabelnya
            # Belum handle kasus attribute < attribute (kyknya ga perlu krn eksotik)
            # Belum handle kasus NEQ (kyknya ga perlu) asumsi aja negligible karena dia tidak spesifik

            if condition.operator == Operator.EQ:
                self.n *= min(1 / QOData().get_V(condition.left_attribute, condition.left_table_name), 1 / QOData().get_V(condition.right_attribute, condition.right_table_name)) # Menurut buku, aman diasumsikan bahwa distribusinya uniform
            if condition.operator in [Operator.LESS, Operator.LESS_EQ]:
                self.n *= (float(condition.right_operand) - QOData().get_min(condition.left_attribute, condition.left_table_name)) / (QOData().get_max(condition.left_attribute, condition.left_table_name) - QOData().get_min(condition.left_attribute, condition.left_table_name))
            if condition.operator in [Operator.GREATER,Operator.GREATER_EQ]:
                self.n *= (QOData().get_max(condition.left_attribute, condition.left_table_name) - float(condition.right_operand) ) / (QOData().get_max(condition.left_attribute, condition.left_table_name) - QOData().get_min(condition.left_attribute, condition.left_table_name))
        self.n = int(self.n)

        self.b = int(1 / (1 / left.b + 1 / right.b))

    def estimate_cost(self, statistics: Dict) -> float:
        # Lagi integrasi fungsi calculate_join_cost biar menggunakan estimate_size
        return self._calculate_operation_cost(statistics)

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        # Placeholder implementation for natural joins
        return 1

    def __str__(self) -> str:
        return f"NATURAL JOIN [{self.algorithm.value}]"


