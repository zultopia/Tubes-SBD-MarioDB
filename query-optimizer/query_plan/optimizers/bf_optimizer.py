from typing import List
from .optimizer import QueryPlanOptimizer
from generator import generate_possible_plans
from equivalence_rules import EquivalenceRules
import random

# Only mock
def get_cost(query: 'QueryPlan') -> float:
    return random.randint(0, 100)

class BFOptimizer(QueryPlanOptimizer):
    def optimize(self, query: 'QueryPlan') -> 'QueryPlan':
        best_plan = query.clone()
        possible_plans = generate_possible_plans(query, [
            EquivalenceRules.deconstruct_conjunction, # Rule 1
            EquivalenceRules.commute_selections, # Rule 2
            EquivalenceRules.collapse_projections, # Rule 3
            EquivalenceRules.combineJoinCondition, # Rule 4
            EquivalenceRules.switchChildrenJoin, # Rule 5
            EquivalenceRules.associativeJoins, # Rule 6
            EquivalenceRules.push_projections_into_join, # Rule 8
        ])

        best_cost = get_cost(best_plan)
        for plan in possible_plans:
            current_cost = get_cost(plan)
            if current_cost < best_cost:
                best_plan = plan
                best_cost = current_cost
                
        
        return best_plan

