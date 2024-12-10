# base.py
from abc import ABC, abstractmethod
from typing import Dict, Union, List
from utils import Pair, Prototype
from .enums import NodeType
import uuid

class QueryNode(ABC, Prototype):
    node_type: NodeType
    children: Union['QueryNode', Pair['QueryNode', 'QueryNode'], None, List['QueryNode']]
    id: str

    def __init__(self, node_type: NodeType):
        self.node_type = node_type
        self.id = str(uuid.uuid4())

    def switchChildren(self) -> None:
        if isinstance(self.children, Pair):
            self.children.first, self.children.second = self.children.second, self.children.first

    def replaceParent(self) -> None:
        self, self.children = self.children, self

    def hasOneChild(self) -> bool:
        return self.children is not None and isinstance(self.children, QueryNode)

    @abstractmethod
    def estimate_cost(self, statistics: Dict) -> float:
        pass

    @abstractmethod
    def _calculate_operation_cost(self, statistics: Dict) -> float:
        pass

    @abstractmethod
    def __str__(self) -> str:
        pass

    @abstractmethod
    def clone(self):
        pass

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, QueryNode):
            return False
        if self.node_type != other.node_type:
            return False
        # Compare children and node-specific attributes in subclasses
        return True

    def __hash__(self):
        # Combine node_type, children, and other attributes into a hash
        # For children, you might do something like:
        # hash_value = hash((self.node_type, tuple_of_children_hashes, tuple_of_node_specific_attributes))
        return hash(self.node_type)  # Extend this in subclasses