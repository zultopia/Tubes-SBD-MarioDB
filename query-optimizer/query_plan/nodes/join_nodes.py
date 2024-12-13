from typing import List, Literal, Dict, Optional
from ..base import QueryNode
from ..enums import NodeType, JoinAlgorithm
from utils import Pair
from ..shared import Condition
from ..enums import Operator

class JoinNode(QueryNode):
    def __init__(self, algorithm: JoinAlgorithm = JoinAlgorithm.NESTED_LOOP):
        super().__init__(NodeType.JOIN)
        self.algorithm = algorithm
        self.children = None  # Should be of type Pair[QueryNode, QueryNode]
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

    def estimate_cost(self, statistics: Dict) -> float:
        return self._calculate_operation_cost(statistics)

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        # Placeholder for actual cost calculation based on join algorithm
        return 1.0

    def __str__(self) -> str:
        return f"JOIN [{self.algorithm.value}]"

    def attributes(self) -> List[str]:
        """
        Returns the combined attributes from both child nodes.
        Assumes that attribute deduplication is handled in ProjectNode.
        """
        if not self.children:
            raise ValueError("JoinNode has no children.")

        left_attributes = self.children.first.attributes()
        right_attributes = self.children.second.attributes()

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

    def __str__(self) -> str:
        if not self.conditions:
            return super().__str__()
        conditions_str = ', '.join([f"{c.left_operand} {c.operator.value} {c.right_operand}" for c in self.conditions])
        return f"JOIN [{self.algorithm.value}] ON {conditions_str}"

    def attributes(self) -> List[str]:
        """
        Returns the combined attributes from both child nodes.
        Assumes that attribute deduplication is handled in ProjectNode.
        """
        return super().attributes()



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

    def __str__(self) -> str:
        return f"NATURAL JOIN [{self.algorithm.value}]"

    def attributes(self) -> List[str]:
        """
        Returns the combined attributes from both child nodes,
        excluding duplicate common attributes.
        """
        if not self.children:
            raise ValueError("NaturalJoinNode has no children.")

        left_attributes = self.children.first.attributes()
        right_attributes = self.children.second.attributes()

        # Extract attribute names without table prefixes
        left_attr_names = {attr.split('.')[-1] for attr in left_attributes}
        right_attr_names = {attr.split('.')[-1] for attr in right_attributes}
        common_attrs = left_attr_names.intersection(right_attr_names)

        # Remove duplicates: include one version without table prefix for common attributes
        combined_attributes = []
        seen = set()

        for attr in left_attributes:
            attr_name = attr.split('.')[-1]
            if attr_name in common_attrs and attr_name not in seen:
                combined_attributes.append(attr_name)  # Add without table prefix
                seen.add(attr_name)
            elif attr_name not in common_attrs:
                combined_attributes.append(attr)

        for attr in right_attributes:
            attr_name = attr.split('.')[-1]
            if attr_name not in common_attrs:
                combined_attributes.append(attr)

        self._cached_attributes = combined_attributes
        return self._cached_attributes
