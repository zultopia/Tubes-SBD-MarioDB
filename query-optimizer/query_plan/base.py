# base.py
from abc import ABC, abstractmethod
from typing import Dict, Union
from utils import Pair
from .enums import NodeType

class QueryNode(ABC):
    node_type: NodeType
    children: Union['QueryNode', Pair['QueryNode', 'QueryNode'], None]

    def __init__(self, node_type: NodeType):
        self.node_type = node_type
    
    #for rule 5,6
    def switchChildren(self):
        self.children.first,self.children.second = self.children.second,self.children.first

    #for rule 2
    def replaceParent(self):
        self,self.children=self.children,self

    @abstractmethod
    def estimate_cost(self, statistics: Dict) -> float:
        pass
    
    @abstractmethod
    def _calculate_operation_cost(self, statistics: Dict) -> float:
        pass

    def __str__(self) -> str:
        return self.node_type.value
