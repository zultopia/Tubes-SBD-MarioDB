from equivalence_rules import EquivalenceRules
from typing import List
from query_plan.base import QueryPlan



def generate_possible_plans(query: 'QueryPlan') -> List['QueryPlan']:
   rules = [EquivalenceRules.deconstruct_conjunction]
   seen = set()
   result = []

   def dfs(node: 'QueryNode'):
       node_str = str(node)
       if node_str in seen:
           return
       
       seen.add(node_str)
       result.append(QueryPlan(node))

       for rule in rules:
           new_nodes = rule(node)
           for new_node in new_nodes:
               dfs(new_node)

   dfs(query.root)
   return result
