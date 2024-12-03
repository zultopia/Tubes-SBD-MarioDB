# base.py
from abc import ABC, abstractmethod
from typing import Dict, Union, List
from utils import Pair, Prototype
from .enums import NodeType

class QueryNode(ABC, Prototype):
    node_type: NodeType
    children: Union['QueryNode', Pair['QueryNode', 'QueryNode'], None, List['QueryNode']]

    def __init__(self, node_type: NodeType):
        self.node_type = node_type
    
    def switchChildren(self) -> None:
        if(isinstance(self.children,Pair)):
            self.children.first,self.children.second = self.children.second,self.children.first

    def replaceParent(self) -> None:
        self,self.children=self.children,self

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

