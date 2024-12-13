import sys
sys.setrecursionlimit(1000) # tambahin jika perlu
from parse_tree import ParseTree
from query_optimizer import get_parse_tree
from from_parse_tree import from_parse_tree
from query_plan.optimizers.bf_optimizer import BFOptimizer
from generator import generate_possible_plans
from equivalence_rules import EquivalenceRules



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
#     print(query_plan)


if current == 'gana':
    query_string = ""
    print("Please enter your query (end with a semicolon ';'):")

    while True:
        line = input().strip()
        if line.endswith(';'):
            query_string += line
            break
        query_string += line + " "
    
    query_string = query_string.strip()  # Final clean-up of leading/trailing spaces

    # try:
    print ("Query is: ")
    print(query_string)
    tree = get_parse_tree(query_string)
    query_plan = from_parse_tree(tree)

    print("Initial plan:")
    print(query_plan)
    print(query_plan.clone())
    
    plans = generate_possible_plans(query_plan)

    # print("\n\n\nGenerated plans:" + str(len(plans)))
    
    # LIMIT_5 = min(len(plans), 20)
    # for plan in plans[:LIMIT_5]:
    #     print(plan)

    bf = BFOptimizer()
    bestPlan = bf.optimize(query_plan)
    print("\n\n\nBest plan:")
    print(bestPlan)
        
    # except Exception as e:
        
    #     print(f"An error occurred: {e}")
    #     print("Please try again with a valid query.")
elif (current == 'azmi'):
    pass
    # from query_plan.query_plan import QueryPlan
    # from query_plan.nodes.project_node import ProjectNode
    # from query_plan.nodes.selection_node import SelectionNode
    # from query_plan.nodes.selection_node import Condition
    # from query_plan.nodes.table_node import TableNode
    # from query_plan.enums import Operator

    # # Create the table node
    # table_node = TableNode("b")

    # # Create a chain of SelectionNodes:
    # # Conditions: a = b, b = c, c = d, d = e, e = f, f = g
    # cond_a_b = Condition("a", "b", Operator.EQ)
    # cond_b_c = Condition("b", "c", Operator.EQ)
    # cond_c_d = Condition("c", "d", Operator.EQ)
    # cond_d_e = Condition("d", "e", Operator.EQ)


    # select_d_e = SelectionNode([cond_d_e])

    # select_c_d = SelectionNode([cond_c_d])
    # select_c_d.set_child(select_d_e)

    # select_b_c = SelectionNode([cond_b_c])
    # select_b_c.set_child(select_c_d)

    # select_a_b = SelectionNode([cond_a_b])
    # select_a_b.set_child(select_b_c)

    # # Create a project node on top
    # project_node = ProjectNode(["a"])
    # project_node.set_child(select_a_b)

    # # Wrap in a QueryPlan
    # plan = QueryPlan(project_node)

    # plans = generate_possible_plans(plan)
    # print("\n\n\nGenerated plans:" + str(len(plans)))
    # for plan in plans:
    #     plan)
