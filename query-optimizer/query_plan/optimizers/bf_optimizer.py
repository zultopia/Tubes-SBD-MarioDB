from typing import List
from .optimizer import QueryPlanOptimizer

class BFOptimizer(QueryPlanOptimizer):
    def optimize(self, query: 'QueryPlan') -> 'QueryPlan':
        pass

    
    def _find_best_plan(self, plans: List['QueryPlan']) -> 'QueryPlan':
        # Implementation here
        pass