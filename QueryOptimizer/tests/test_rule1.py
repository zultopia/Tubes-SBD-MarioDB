from generator import generate_possible_plans
from equivalence_rules import EquivalenceRules

class TestOptimizerRule1:
    def test_1(self):
        from query_plan.query_plan import QueryPlan
        from query_plan.nodes.project_node import ProjectNode
        from query_plan.nodes.selection_node import SelectionNode
        from query_plan.nodes.selection_node import Condition
        from query_plan.nodes.table_node import TableNode
        from query_plan.enums import Operator

        # Create the table node
        table_node = TableNode("b")

        # Create a chain of SelectionNodes:
        # Conditions: a = b, b = c, c = d, d = e, e = f, f = g
        cond_a_b = Condition("a", "b", Operator.EQ)
        cond_b_c = Condition("b", "c", Operator.EQ)
        cond_c_d = Condition("c", "d", Operator.EQ)
        cond_d_e = Condition("d", "e", Operator.EQ)


        select_d_e = SelectionNode([cond_d_e])
        select_d_e.set_child(table_node)

        select_c_d = SelectionNode([cond_c_d])
        select_c_d.set_child(select_d_e)

        select_b_c = SelectionNode([cond_b_c])
        select_b_c.set_child(select_c_d)

        select_a_b = SelectionNode([cond_a_b])
        select_a_b.set_child(select_b_c)

        # Create a project node on top
        project_node = ProjectNode(["a"])
        project_node.set_child(select_a_b)

        # Wrap in a QueryPlan
        plan = QueryPlan(project_node)

        plans = generate_possible_plans(plan, [
            EquivalenceRules.deconstruct_conjunction
        ])
        assert len(plans) == 1
        print("test_1 passed")
    
    def test_2(self):
        from query_plan.query_plan import QueryPlan
        from query_plan.nodes.project_node import ProjectNode
        from query_plan.nodes.selection_node import SelectionNode
        from query_plan.nodes.selection_node import Condition
        from query_plan.nodes.table_node import TableNode
        from query_plan.enums import Operator

        # Create the table node
        table_node = TableNode("b")

        # Create a chain of SelectionNodes:
        # Conditions: a = b, b = c, c = d, d = e, e = f, f = g
        cond_a_b = Condition("a", "b", Operator.EQ)
        cond_b_c = Condition("b", "c", Operator.EQ)
        cond_c_d = Condition("c", "d", Operator.EQ)
        cond_d_e = Condition("d", "e", Operator.EQ)
        cond_e_f = Condition("e", "f", Operator.EQ)
        cond_f_g = Condition("f", "g", Operator.EQ)


        selection = SelectionNode([cond_a_b, 
                                      cond_b_c,
                                        cond_c_d])
                                   

        # Create a project node on top
        project_node = ProjectNode(["a"])
        project_node.set_child(selection)

        # Wrap in a QueryPlan
        plan = QueryPlan(project_node)

        plans = generate_possible_plans(plan, [
            EquivalenceRules.deconstruct_conjunction
        ])
        assert len(plans) == 10
        print("test_2 passed")