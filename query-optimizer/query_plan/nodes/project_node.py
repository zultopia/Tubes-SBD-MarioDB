# project_node.py

from typing import List, Dict, Optional
from ..base import QueryNode
from ..enums import NodeType

class ProjectNode(QueryNode):
    def __init__(self, attributes: List[str]):
        super().__init__(NodeType.PROJECT)
        self.original_attributes = sorted(attributes)  # Store original projection list
        self.projected_attributes = []  # To be populated with fully qualified attributes
        self.child = None  # Single child node
        self._attributes = self.projected_attributes.copy()  # Cache the projected attributes

    def set_child(self, child: QueryNode):
        self.child = child
        self.process_attributes()

    def process_attributes(self):
        """
        Processes the original attributes to fully qualify them based on the child node's attributes.
        Expands '*' to include all child attributes.
        """
        if not self.child:
            raise ValueError("ProjectNode must have a child node before processing attributes.")

        child_attrs = self.child.attributes()
        if '*' in self.original_attributes:
            # Expand '*' to all child attributes
            self.projected_attributes = sorted(child_attrs)
            return

        # Qualify attributes
        qualified_attrs = []
        for attr in self.original_attributes:
            if attr in child_attrs:
                # Attribute is already fully qualified
                qualified_attrs.append(attr)
            else:
                # Attempt to match unqualified attribute
                matches = [child_attr for child_attr in child_attrs if child_attr.split('.')[-1] == attr]
                if matches:
                    # Allow ambiguity by including all matches
                    qualified_attrs.extend(matches)
                else:
                    raise ValueError(f"Attribute '{attr}' not found in child node's attributes.")
        
        # Ensure the attributes are unique and sorted

        self.projected_attributes = sorted(set(qualified_attrs))



    def clone(self) -> 'ProjectNode':
        cloned_attributes = self.original_attributes.copy()
        cloned_node = ProjectNode(cloned_attributes)
        cloned_node.id = self.id
        if self.child:
            cloned_node.set_child(self.child.clone())
        return cloned_node

    def estimate_cost(self, statistics: Dict) -> float:
        return self._calculate_operation_cost(statistics)

    def _calculate_operation_cost(self, statistics: Dict) -> float:
        # Placeholder implementation for project cost
        return 1

    def __str__(self) -> str:
        return f"PROJECT {', '.join(self.projected_attributes)}"

    def attributes(self) -> List[str]:
        """
        Returns the list of fully qualified attributes this ProjectNode projects.
        """
        return self.projected_attributes.copy()
