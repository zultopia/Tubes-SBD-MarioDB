import sys
sys.setrecursionlimit(1000) # tambahin jika perlu
from parse_tree import ParseTree
from query_optimizer import get_parse_tree
from from_parse_tree import from_parse_tree
from query_plan.optimizers.bf_optimizer import BFOptimizer
from generator import generate_possible_plans



current = 'gana'

# from query_plan.nodes.table_node import TableNode
# from query_plan.nodes.join_nodes import ConditionalJoinNode, NaturalJoinNode, JoinCondition
# from query_plan.nodes.sorting_node import SortingNode
# from query_plan.nodes.project_node import ProjectNode
# from query_plan.enums import JoinAlgorithm
# from utils import Pair
# from query_plan.query_plan import QueryPlan



# if __name__ == "__main__":
#     employees = TableNode("employees")
#     departments = TableNode("departments")
#     salaries = TableNode("salaries")
    
#     join1 = ConditionalJoinNode(
#         JoinAlgorithm.HASH,
#         [JoinCondition("department_id", "id", "=")]
#     )
#     join1.set_children(Pair(employees, departments))
    
#     join2 = NaturalJoinNode(JoinAlgorithm.MERGE)
#     join2.set_children(Pair(join1, salaries))
    
#     sort = SortingNode(["salary"])
#     sort.set_child(join2)
    
#     project = ProjectNode(["name", "department_name", "salary"])
#     project.set_child(sort)
    
#     query_plan = QueryPlan(project)
#     query_plan.print()


if (current == 'gana'):
    query_string = input('Please enter your query: ')
    tree = get_parse_tree(query_string)
    query_plan = from_parse_tree(tree)
    query_plan.print()
    #print(tree)

    plans = generate_possible_plans(query_plan)
    print("\n\n\nGenerated plans:" + str(len(plans)))
    LIMIT_5 = min(len(plans), 5)
    for plan in plans[:LIMIT_5]:
        plan.print()
        
    # bf = BFOptimizer()
    # plans = bf.generate_possible_plans(query_plan)
    # for plan in plans:
    #     plan.print()