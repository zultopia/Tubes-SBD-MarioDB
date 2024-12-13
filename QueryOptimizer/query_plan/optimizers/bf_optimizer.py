from typing import List
from QueryOptimizer.data import MOCK_DATA
from QueryOptimizer.query_plan.optimizers.optimizer import QueryPlanOptimizer
from QueryOptimizer.generator import generate_possible_plans
from QueryOptimizer.equivalence_rules import EquivalenceRules
import random

# Only mock
def get_cost(query: 'QueryPlan') -> float:
    return random.randint(0, 100)

class BFOptimizer(QueryPlanOptimizer):
    def optimize(self, query: 'QueryPlan') -> 'QueryPlan':
        best_plan = query.clone()
        possible_plans = generate_possible_plans(query)

        best_cost = best_plan.estimate_cost(MOCK_DATA)
        for plan in possible_plans:
            current_cost = plan.estimate_cost(MOCK_DATA)
            if current_cost < best_cost:
                best_plan = plan
                best_cost = current_cost
                
        
        return best_plan

