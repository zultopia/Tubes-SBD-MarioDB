from typing import List, Dict

from data import BLOCK_SIZE, QOData
from ..base import QueryNode
from ..enums import NodeType
import uuid

class ProjectNode(QueryNode):
    def __init__(self, attributes: List[str]):
        super().__init__(NodeType.PROJECT)
        # Sort the attributes to ensure consistent ordering
        attributes.sort()

        self.project_list = attributes

        self.child = None  # Single child node

    def set_child(self, child: QueryNode):
        self.child = child

    def clone(self) -> 'ProjectNode':
        cloned_attributes = self.attributes.copy()
        cloned_child = self.child.clone() if self.child else None
        cloned_node = ProjectNode(cloned_attributes)
        cloned_node.id = self.id

        if cloned_child:
            cloned_node.set_child(cloned_child)
        return cloned_node
    
    def estimate_size(self, statistics: Dict, alias_dict):
        if not self.child:
            return
        self.child.estimate_size()


        self.attributes = []
        for attr in self.project_list:
            for i in self.child.attributes:
                child_attribute, child_alias = i
                if child_attribute == attr:
                    self.attributes.append((child_attribute, child_alias))
        
        self.n = self.child.n

        record_size = 0
        for i in self.child.attributes:
            attribute, alias = i.first, i.second
            table = alias_dict[alias]
            record_size += QOData().get_size(attribute, table)
        self.b = int(BLOCK_SIZE / record_size)


    def estimate_cost(self, statistics: Dict, alias_dict) -> float:
        return self.child.estimate_cost()


    def __str__(self) -> str:
        return f"PROJECT {', '.join(self.attributes)}"