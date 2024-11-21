from query_plan import *
from abc import ABC, abstractmethod

class QueryPlanOptimizer(ABC):
    """
    An ABC for query plan optimization algorithms.
    """
    @abstractmethod
    def optimize(self, query: QueryPlan) -> QueryPlan:
        """
        Optimize the query plan tree to reduce execution cost.
        
        Args:
            query: Parsed query tree to optimize
        
        Returns:
            Optimized query plan tree
        """
        pass

class BFOptimizer(QueryPlanOptimizer):
    def optimize(self, query: QueryPlan) -> QueryPlan:
        kopi = query.clone()
        return kopi

if __name__ == "__main__":
    optimizer = BFOptimizer()
    optimizer.optimize(QueryPlan(QueryNode("PROJECT")))
    pass