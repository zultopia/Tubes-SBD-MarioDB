from typing import List
from .optimizer import QueryPlanOptimizer
from generator import generate_possible_plans
from equivalence_rules import EquivalenceRules
from data import MOCK_DATA
import random

# Only mock
def get_cost(query: 'QueryPlan') -> float:
    return random.randint(0, 100)

class BFOptimizer(QueryPlanOptimizer):
    def optimize(self, query: 'QueryPlan') -> 'QueryPlan':
        best_plan = query.clone()
        possible_plans = generate_possible_plans(query)
        for plan in possible_plans:
            print(f"Plan: \n{plan}")

        best_cost = best_plan.estimate_cost(MOCK_DATA)
        for plan in possible_plans:
            current_cost = plan.estimate_cost(MOCK_DATA)
            print(f"Current cost: {current_cost}")
            if current_cost < best_cost:
                best_plan = plan
                best_cost = current_cost
                
        
        return best_plan

