from typing import List, Literal, Dict, Optional
from QueryOptimizer.query_plan.base import QueryNode
from QueryOptimizer.query_plan.enums import NodeType, JoinAlgorithm
from QueryOptimizer.utils import Pair
from QueryOptimizer.query_plan.shared import Condition
from QueryOptimizer.query_plan.enums import Operator
from QueryOptimizer.query_plan.nodes.constants import *
from QueryOptimizer.data import QOData

class JoinNode(QueryNode):
    def __init__(self, algorithm: JoinAlgorithm = JoinAlgorithm.NESTED_LOOP):
        super().__init__(NodeType.JOIN)
        self.algorithm = algorithm
        self.children: Pair[QueryNode, QueryNode] = None  # Should be of type Pair[QueryNode, QueryNode]
        self.child = None # Join tidak pakai child
        self.table_name = None
        self._cached_attributes: Optional[List[str]] = None  # Cache for attributes

    def set_children(self, children: 'Pair[QueryNode, QueryNode]'):
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

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        # Placeholder implementation
        return 1

    def __str__(self) -> str:
        return f"JOIN [{self.algorithm.value}]"

    def get_node_attributes(self) -> List[Pair[str, str]]:
        """
        Returns the combined attributes from both child nodes.
        Assumes that attribute deduplication is handled in ProjectNode.
        """
        if not self.children:
            raise ValueError("JoinNode has no children.")

        left_attributes = self.children.first.get_node_attributes()
        right_attributes = self.children.second.get_node_attributes()

        # Combine attributes
        combined_attributes = left_attributes + right_attributes
        return combined_attributes




class ConditionalJoinNode(JoinNode):
    def __init__(self, algorithm: JoinAlgorithm = JoinAlgorithm.NESTED_LOOP, conditions: Optional[List['Condition']] = None):
        super().__init__(algorithm)
        self.conditions = sorted(
            conditions if conditions else [],
            key=lambda c: (c.operator != 'EQ', c.left_operand, c.right_operand)
        )
        self._cached_attributes: Optional[List[str]] = None  # Cache for attributes

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
        assert (isinstance(self.children, Pair))
        
        left: QueryNode = self.children.first
        right: QueryNode = self.children.second
        left.estimate_size(statistics, alias_dict)
        right.estimate_size(statistics, alias_dict)

        
        left_attributes = left.attributes
        right_attributes = right.attributes
        
        self.attributes = left_attributes + right_attributes

        self.n = left.n * right.n
        for condition in self.conditions:
            left_table_name = alias_dict[condition.left_table_alias]
            

            if condition.operator == Operator.EQ:
                if "'" in condition.right_operand:
                    self.n *= 1 / QOData().get_V(condition.left_attribute, left_table_name)
                else:
                    right_table_name = alias_dict[condition.right_table_alias]
                    self.n *= min(1 / QOData().get_V(condition.left_attribute, left_table_name), 1 / QOData().get_V(condition.right_attribute, right_table_name)) # Menurut buku, aman diasumsikan bahwa distribusinya uniform
            if condition.operator in [Operator.LESS, Operator.LESS_EQ]:
                self.n *= (float(condition.right_operand) - QOData().get_min(condition.left_attribute, left_table_name)) / (QOData().get_max(condition.left_attribute, left_table_name) - QOData().get_min(condition.left_attribute, left_table_name))
            if condition.operator in [Operator.GREATER,Operator.GREATER_EQ]:
                self.n *= (QOData().get_max(condition.left_attribute, left_table_name) - float(condition.right_operand) ) / (QOData().get_max(condition.left_attribute, left_table_name) - QOData().get_min(condition.left_attribute, left_table_name))
        
        if self.n < 0:
            self.n = 0

        self.n = int(self.n)
        
        self.b = int(1 / (1 / (left.b + 1) + 1 / (right.b + 1)))



    def estimate_cost(self, statistics: Dict, alias_dict) -> float:
        self.estimate_size(statistics, alias_dict)

        left: QueryNode = self.children.first
        right: QueryNode = self.children.second

        previous_cost = left.estimate_cost(statistics, alias_dict) + right.estimate_cost(statistics, alias_dict)

        
        if self.algorithm == JoinAlgorithm.NESTED_LOOP:
            is_index = False
            for condition in self.conditions:
                left_table_name = alias_dict[condition.left_table_alias]
                right_table_name = alias_dict[condition.right_table_alias]
                if (QOData().get_index(condition.left_attribute, left_table_name) == 'btree' or QOData().get_index(condition.right_attribute, right_table_name) == 'btree') and condition.operator != Operator.NEQ:
                    is_index = True
                    break
            
            if not is_index:
                return previous_cost + (left.b + right.b) * t_T + 2 * t_S + self.b * t_T
            else:
                # TODO: Height dari tree perlu dicari
                c = 3
                return previous_cost + left.b * (t_T + t_S) + left.n * c + self.b * t_T
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
                return previous_cost + (left.b + right.b) * t_T + 2 * t_S + self.b * t_T
        elif self.algorithm == JoinAlgorithm.MERGE:
            # No seeks required because b_b -> infinity implies seeks -> 0 
            return previous_cost + (left.b + right.b) * t_T + self.b * t_T
        


    def __str__(self) -> str:
        if not self.conditions:
            return super().__str__()
        conditions_str = ', '.join([f"{c.left_operand} {c.operator.value} {c.right_operand}" for c in self.conditions])
        return f"JOIN [{self.algorithm.value}] ON {conditions_str}"

    def get_node_attributes(self) -> List[Pair[str, str]]:
        """
        Returns the combined attributes from both child nodes.
        Assumes that attribute deduplication is handled in ProjectNode.
        """
        return super().get_node_attributes()



class NaturalJoinNode(JoinNode):
    def __init__(self, algorithm: JoinAlgorithm = JoinAlgorithm.NESTED_LOOP):
        super().__init__(algorithm)
        self._cached_attributes: Optional[List[str]] = None

    def clone(self) -> 'NaturalJoinNode':
        cloned_children = Pair(self.children.first.clone(), self.children.second.clone()) if self.children else None
        cloned_node = NaturalJoinNode(self.algorithm)
        cloned_node.id = self.id
        cloned_node.set_children(cloned_children)
        return cloned_node

    def estimate_size(self, statistics: Dict, alias_dict):
        assert (isinstance(self.children, Pair))
        
        left: QueryNode = self.children.first
        right: QueryNode = self.children.second
        left.estimate_size(statistics, alias_dict)
        right.estimate_size(statistics, alias_dict)

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
        for attr, alias in common:
            table = alias_dict[alias]
            self.n *= 1 / QOData().get_V(attr, table) # Menurut buku, aman diasumsikan bahwa distribusinya uniform

        self.n = int(self.n)

        if self.n < 0:
            self.n = 0

        self.b = int(1 / (1 / (left.b + 1) + 1 / (right.b + 1)))

    def estimate_cost(self, statistics: Dict, alias_dict) -> float:
        self.estimate_size(statistics, alias_dict)

        left: QueryNode = self.children.first
        right: QueryNode = self.children.second

        previous_cost = left.estimate_cost(statistics, alias_dict) + right.estimate_cost(statistics, alias_dict)

        left_attributes = left.attributes
        right_attributes = right.attributes

        
        if self.algorithm == JoinAlgorithm.NESTED_LOOP:
            is_index = False
            common = []
            for i in left_attributes:
                left_attribute, left_alias = i
                for j in right_attributes: 
                    right_attribute, _ = j
                    if left_attribute == right_attribute and not any(left_attribute == attr for attr, _ in common):
                        common.append((left_attribute, left_alias))
            for attr, alias in common:
                table = alias_dict[alias]
                if QOData().get_index(attr, table) == 'btree':
                    is_index = True
                    break

            if not is_index:
                return previous_cost + (left.b + right.b) * t_T + 2 * t_S + self.b * t_T
            else:
                # Todo: Height dari tree perlu dicari
                c = 3
                return previous_cost + left.b * (t_T + t_S) + left.n * c + self.b * t_T
        elif self.algorithm == JoinAlgorithm.HASH:
            is_index = False
            common = []
            for i in left_attributes:
                left_attribute, left_alias = i.first, i.second
                for j in right_attributes: 
                    right_attribute, right_alias = j.first, j.second
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
                return previous_cost + (left.b + right.b) * t_T + 2 * t_S + self.b * t_T
        elif self.algorithm == JoinAlgorithm.MERGE:
            # No seeks required because b_b -> infinity implies seeks -> 0 
            return previous_cost + (left.b + right.b) * t_T + self.b * t_T
    

    def __str__(self) -> str:
        return f"NATURAL JOIN [{self.algorithm.value}]"

    def get_node_attributes(self) -> List[str]:
        """
        Returns the combined attributes from both child nodes.
        For common attributes, keeps only the left table's qualified version.
        All attributes maintain their table prefixes.
        """
        if not self.children:
            raise ValueError("NaturalJoinNode has no children.")

        left_attributes = self.children.first.get_node_attributes()
        right_attributes = self.children.second.get_node_attributes()

        # Extract attribute names without table prefixes but keep mapping to full names
        left_attr_map = {attr.split('.')[-1]: attr for attr in left_attributes}
        right_attr_map = {attr.split('.')[-1]: attr for attr in right_attributes}
        
        # Find common attribute names
        common_attrs = set(left_attr_map.keys()) & set(right_attr_map.keys())

        # Build combined attributes list
        combined_attributes = []
        
        # Add all left attributes
        combined_attributes.extend(left_attributes)
        
        # Add right attributes that aren't common
        for attr_name, full_attr in right_attr_map.items():
            if attr_name not in common_attrs:
                combined_attributes.append(full_attr)

        self._cached_attributes = combined_attributes
        return self._cached_attributes
