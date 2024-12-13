# base.py
from abc import ABC, abstractmethod
from typing import Dict, Tuple, Union, List
from QueryOptimizer.utils import Pair, Prototype
from .enums import NodeType
import uuid

class QueryNode(ABC, Prototype):
    node_type: NodeType
    children: Union['QueryNode', Pair['QueryNode', 'QueryNode'], None, List['QueryNode']]
    id: str

    alias: str | None # str for table, None for non-table expressions
    attributes: List[Pair[str, str]] # (attribute name, original table alias). 'attributes' can contain the same attribute names but must have different aliases. 
    n: int # The exact number of tuples for tables or the approximated number of tuples for the result of the operations.
    b: int # The blocking factor (the number of records that fit inside a block)
    # f: int # The number of blocks or n / b


    def __init__(self, node_type: NodeType):
        self.node_type = node_type
        self.id = str(uuid.uuid4())
        self.child = None
        self.children = None

    def switchChildren(self) -> None:
        if isinstance(self.children, Pair):
            self.children.first, self.children.second = self.children.second, self.children.first

    def replaceParent(self) -> None:
        self, self.children = self.children, self

    def hasOneChild(self) -> bool:
        return self.children is not None and isinstance(self.children, QueryNode)

    @abstractmethod
    def estimate_size(self, statistics: Dict, alias_dict: Dict[str, str]):
        pass

    @abstractmethod
    def estimate_cost(self, statistics: Dict, alias_dict: Dict[str, str]) -> float:
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

    @abstractmethod
    def get_node_attributes(self) -> List[Pair[str, str]]:
        raise NotImplementedError