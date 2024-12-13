# project_node.py

import copy
from typing import List, Dict, Optional

from QueryOptimizer.data import QOData
from QueryOptimizer.query_plan.base import QueryNode
from QueryOptimizer.query_plan.nodes.constants import *
from QueryOptimizer.query_plan.enums import NodeType
from QueryOptimizer.data import QOData
from QueryOptimizer.query_plan.nodes.constants import BLOCK_SIZE
from QueryOptimizer.utils import Pair

class ProjectNode(QueryNode):
    def __init__(self, attributes: List[str]):
        super().__init__(NodeType.PROJECT)
        self.original_attributes = sorted(attributes)  # Store original projection list
        self.projected_attributes = []  # To be populated with fully qualified attributes
        # Sort the attributes to ensure consistent ordering
        attributes.sort()

        self.project_list = attributes

        self.child = None  # Single child node
        self.children = None
        self._attributes = self.projected_attributes.copy()  # Cache the projected attributes

    def set_child(self, child: QueryNode):
        self.child = child
        self.process_attributes()

    def process_attributes(self):
        """
        Processes the original attributes to fully qualify them based on the child node's attributes.
        Handles '*' expansion and ambiguity resolution.
        """
        if not self.child:
            raise ValueError("ProjectNode must have a child node before processing attributes.")

        child_attrs = copy.deepcopy(self.child.get_node_attributes())
        # Reset projected attributes
        self.projected_attributes = []

        if '*' in self.original_attributes:
            # Expand '*' to all child attributes
            self.projected_attributes = copy.deepcopy(child_attrs)
            return
        # Process each attribute in the original list
        for attr in self.original_attributes:
            
            if '.' in attr:  # Fully qualified attribute (e.g., "s.name")
                query_alias, query_attribute = attr.split('.')
                
                for leave in child_attrs:
                    leave_alias, leave_attribute = leave.split('.')
                    assert (leave_alias != '.')
                    if query_attribute == leave_attribute:
                        self.projected_attributes.append(leave_alias + "." + leave_attribute)
                        break

            else:  # Unqualified attribute (e.g., "name")
                # Find matching attributes in child attributes
                matches = [child_attr for child_attr in child_attrs 
                         if child_attr.split('.')[-1] == attr]
                
                if not matches:
                    raise ValueError(f"Attribute '{attr}' not found in child node's attributes.")
                if len(matches) > 1:
                    raise ValueError(f"Ambiguous attribute reference '{attr}'. Please qualify with table alias.")
                self.projected_attributes.extend(matches)
        
        self.project_list = self.projected_attributes


    def clone(self) -> 'ProjectNode':
        cloned_attributes = self.original_attributes.copy()
        cloned_node = ProjectNode(cloned_attributes)
        cloned_node.id = self.id
        if self.child:
            cloned_node.set_child(self.child.clone())
        return cloned_node
    
    def estimate_size(self, statistics: Dict, alias_dict):
        if not self.child:
            return
        self.child.estimate_size(statistics, alias_dict)


        self.attributes = []
        for attr in self.project_list:
            for i in self.child.attributes:
                child_attribute, child_alias = i.first, i.second
                if child_attribute == attr:
                    self.attributes.append((child_attribute, child_alias))
        
        self.n = self.child.n

        record_size = 0
        for i in self.child.attributes:
            attribute, alias = i[0], i[1]
            assert(alias in alias_dict)
            table = alias_dict[alias]
            record_size += QOData().get_size(attribute, table)
        self.b = int(BLOCK_SIZE / record_size)


    def estimate_cost(self, statistics: Dict, alias_dict) -> float:
        self.estimate_size(statistics, alias_dict)

        previous_cost = self.child.estimate_cost(statistics, alias_dict)
        return previous_cost + self.b * t_T


    def __str__(self) -> str:
        return f"PROJECT {', '.join(self.projected_attributes)}"

    def get_node_attributes(self) -> List[Pair[str, str]]:
        """
        Returns the list of fully qualified attributes this ProjectNode projects.
        """
        return self.projected_attributes.copy()
