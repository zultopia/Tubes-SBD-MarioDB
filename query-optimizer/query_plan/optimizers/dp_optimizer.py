from typing import List
from .optimizer import QueryPlanOptimizer

class DPOptimizer(QueryPlanOptimizer):
    def optimize(self, query: 'QueryPlan') -> 'QueryPlan':
        reachable_plans = self.__generate_possible_plans(query)
        return self._find_best_plan(reachable_plans)
    
    def __generate_possible_plans(self, query: 'QueryPlan') -> List['QueryPlan']:
        # Implementation here
        pass
    
    def _find_best_plan(self, plans: List['QueryPlan']) -> 'QueryPlan':
        # Implementation here
        pass