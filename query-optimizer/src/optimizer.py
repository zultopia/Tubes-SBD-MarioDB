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
        reachable_plans = self.__generate_possible_plans(query)
        best_plan = None
        for plan in reachable_plans:
            # Not yet implemented
            pass

        return best_plan
    
    def __generate_possible_plans(self, query: QueryPlan) -> List[QueryPlan]:
        """
        Generate all possible query plans that can be reached from the given query plan.
        
        Args:
            query: Query plan to generate possible plans from
        
        Returns:
            List of possible query plans
        """
        
        # Not yet implemented
        pass

if __name__ == "__main__":
    optimizer = BFOptimizer()
    optimizer.optimize(QueryPlan(QueryNode("PROJECT")))
    pass