from typing import Dict

from data import QOData
from ..base import QueryNode
from ..enums import NodeType

class TableNode(QueryNode):
    def __init__(self, table_name: str, alias: str | None = None):
        super().__init__(NodeType.TABLE)
        self.table_name = table_name
        self.alias = table_name if alias is None else alias
        self.child = None  # Single child node
        self.children = None  # Not used in TableNode

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
        return f"TABLE {self.table_name} AS {self.alias}" if self.alias != self.table_name else f"TABLE {self.table_name}"