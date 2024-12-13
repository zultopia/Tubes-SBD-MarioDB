from typing import Dict, List, Tuple
from ..base import QueryNode
from ..enums import NodeType

class LimitNode(QueryNode):
    limit: int

    def __init__(self, limit: int):
        super().__init__(NodeType.LIMIT)
        self.limit = int(limit)
        self._cached_attributes: List[str] = []
        self.children = None
        
    
    def set_child(self, child: QueryNode):
        self.child = child

    def __str__(self) -> str:
        return f"LIMIT {self.limit}"
    
    def _calculate_operation_cost(self, statistics: Dict) -> float:
        return 1
    
    def estimate_size(self, statistics: Dict, alias_dict) -> float:
        self.child.estimate_size(statistics, alias_dict)

        # n dan b tidak perlu dihitung karena itu step akhir

    def estimate_cost(self, statistics: Dict, alias_dict) -> float:
        self.estimate_size(statistics, alias_dict)

        previous_cost = self.child.estimate_cost(statistics, alias_dict)
        
        return previous_cost 

    def clone(self):
        cloned_node = LimitNode(self.limit)
        if self.child:
            cloned_node.child = self.child.clone()
        cloned_node.id = self.id
        return cloned_node

    def get_node_attributes(self) -> List[str]:
        if not self.child:
            raise ValueError("LimitNode has no child.")
        
        if not self._cached_attributes:
            self._cached_attributes = self.child.get_node_attributes().copy()

        return self._cached_attributes
    
    def estimate_size(self, statistics: Dict, alias_dict: Dict[str, str]):
        #  Todo: Implementasi LimitNode
        return 0
    
    
    

