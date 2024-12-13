
from query_plan.query_plan import QueryPlan
from query_optimizer import get_parse_tree
from from_parse_tree import from_parse_tree
from query_plan.optimizers.bf_optimizer import BFOptimizer

class QueryOptimizerClient:
    def __init__(self):
        pass

    def parse_query(query_string: str) -> QueryPlan:

        try:
            tree = get_parse_tree(query_string)
            query_plan = from_parse_tree(tree)
            return query_plan
        except Exception as e:
            print(f"An error occurred: {e}")
            print("Please try again with a valid query.")
    
    def optimize_query(tree: QueryPlan) -> QueryPlan:
        optimizer = BFOptimizer()
        best_plan = optimizer.optimize(tree)
        return best_plan
    
    def get_cost(tree: QueryPlan) -> float:
        tree.setup()
        return tree.estimate_cost()


