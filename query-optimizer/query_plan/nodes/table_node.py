from typing import List, Dict, Optional
from query_plan.base import QueryNode
from query_plan.enums import NodeType
from data import QOData  
from utils import Pair

class TableNode(QueryNode):
    def __init__(self, table_name: str, alias: Optional[str] = None):
        super().__init__(NodeType.TABLE)
        self.table_name = table_name
        self.alias = alias if alias else table_name  # Use alias if provided
        self.child = None  # Single child node
        self._cached_attributes: Optional[List[str]] = None  # Cache for attributes

    def set_child(self, child: QueryNode):
        self.child = child

    def clone(self) -> 'TableNode':
        cloned_node = TableNode(self.table_name, self.alias)
        cloned_node.id = self.id
        if self.child:
            cloned_node.set_child(self.child.clone())
        return cloned_node

    def set_child(self, child: QueryNode):
        self.child = child

    def estimate_size(self, statistics: Dict, alias_dict):
        attributes = statistics[self.table_name]['attributes']
        self.attributes =  [(attr_name, self.alias) for attr_name, _ in attributes.items()]
        self.n = QOData().get_n(self.table_name)
        self.b = QOData().get_b(self.table_name)


    def estimate_cost(self, statistics: Dict, alias_dict) -> float:
        self.estimate_size()
        return 0

    def __str__(self) -> str:
        return f"TABLE {self.table_name}" + (f" AS {self.alias}" if self.alias != self.table_name else "")

    def get_node_attributes(self) -> List[Pair[str, str]]:
        """
        Returns a list of attributes for the table, prefixed with the table name or alias.
        Caches the result after the first computation.
        """
        if self._cached_attributes is not None:
            return self._cached_attributes

        qo_data = QOData.get_instance()

        if not qo_data.has_relation(self.table_name):
            raise ValueError(f"Relation '{self.table_name}' not found in QOData.")

        attributes = qo_data.get_all_attributes(self.table_name)
        if not attributes:
            raise ValueError(f"No attributes found for relation '{self.table_name}'.")

        # Prefix attributes with table name or alias
        self._cached_attributes = [f"{self.alias}.{attr}" for attr in attributes]
        return self._cached_attributes
