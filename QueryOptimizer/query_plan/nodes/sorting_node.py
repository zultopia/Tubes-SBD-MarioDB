from typing import List, Dict, Optional
from QueryOptimizer.query_plan.base import QueryNode
from QueryOptimizer.query_plan.enums import NodeType
from QueryOptimizer.query_plan.nodes.constants import *
import uuid
from QueryOptimizer.utils import Pair

class SortingNode(QueryNode):
    def __init__(self, attributes: List[str], ascending: bool = True):
        super().__init__(NodeType.SORTING)
        self.sort_attributes = sorted(attributes)  # Sort for consistency
        self.ascending = ascending
        self.child = None  # Single child node
        self.children = None
        self._cached_attributes: Optional[List[str]] = None  # Cache for attributes

    def set_child(self, child: QueryNode):
        self.child = child

    def clone(self) -> 'SortingNode':
        cloned_node = SortingNode(self.sort_attributes.copy(), self.ascending)
        cloned_node.id = self.id
        if self.child:
            cloned_node.set_child(self.child.clone())
        return cloned_node

    def estimate_size(self, statistics: Dict, alias_dict):
        if not self.child:
            return
        self.child.estimate_size()

        self.attributes = self.child.attributes
        self.n = self.child.n
        self.b = self.child.b


    def estimate_cost(self, statistics: Dict, alias_dict) -> float:
        # Sorting tidak memakan IO
        
        self.estimate_size()
        previous_cost = self.child.estimate_cost()

        return previous_cost + self.b * t_T

    def __str__(self) -> str:
        order = "ASC" if self.ascending else "DESC"
        return f"SORT BY {', '.join(self.sort_attributes)} {order}"

    def get_node_attributes(self) -> List[str]:
        print("IN SORTING NODE")
        if not self.child:
            raise ValueError("SortingNode has no child.")
            
        child_attrs = self.child.get_node_attributes()
        
        for attr in self.attributes:
        
            # Find matching attribute from child
            unqualified_attr = attr
            matching_attrs = [attr for attr in child_attrs if attr.split('.')[-1] == unqualified_attr]

            print("IN SORTING NODE")
            print(matching_attrs)
            print(unqualified_attr)
            
            if len(matching_attrs) == 1:
                # Single match - use its table alias
                table_alias = matching_attrs[0].split('.')[0]
                attr = f"{table_alias}.{unqualified_attr}"
                
            elif len(matching_attrs) == 2:
                raise ValueError(f"Ambiguous attribute '{unqualified_attr}' found in child node attributes")
                
            elif len(matching_attrs) > 2:
                raise ValueError(f"Too many matching attributes found for '{unqualified_attr}'")
            else:
                raise ValueError(f"Attribute '{unqualified_attr}' not found in child node attributes")
        
        return child_attrs