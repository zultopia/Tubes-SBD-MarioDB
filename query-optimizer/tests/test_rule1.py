from generator import generate_possible_plans
from equivalence_rules import EquivalenceRules
from time import time
from query_plan.query_plan import QueryPlan
from query_plan.nodes.project_node import ProjectNode
from query_plan.nodes.selection_node import SelectionNode
from query_plan.nodes.selection_node import Condition
from query_plan.nodes.table_node import TableNode
from query_plan.enums import Operator
from utils import Pair
from query_plan.nodes.join_nodes import ConditionalJoinNode, JoinAlgorithm

class TestOptimizerRule1:
    def test_1(self):

        # Record the start time
        start_time = time()

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
        print(f"Test 1: Passed - {time() - start_time:.6f} s")
    
    def test_2(self):

        # Record the start time
        start_time = time()

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
        selection.set_child(table_node)
                                   

        # Create a project node on top
        project_node = ProjectNode(["a"])
        project_node.set_child(selection)

        # Wrap in a QueryPlan
        plan = QueryPlan(project_node)

        plans = generate_possible_plans(plan, [
            EquivalenceRules.deconstruct_conjunction
        ])

        assert len(plans) == 10
        print(f"Test 2: Passed - {time() - start_time:.6f} s")

    def test_single_selection_no_change(self):
        """Test that single selection doesn't get deconstructed"""
        start_time = time()
        
        table_node = TableNode("students")
        selection = SelectionNode([
            Condition("age", "20", Operator.EQ)
        ])
        selection.set_child(table_node)
        project = ProjectNode(["name"])
        project.set_child(selection)
        plan = QueryPlan(project)


        plans = generate_possible_plans(plan, [EquivalenceRules.deconstruct_conjunction])

        assert len(plans) == 1
        print(f"Test single selection: Passed - {time() - start_time:.6f} s")

    def test_two_conditions(self):
        """Test deconstructing two AND conditions"""
        start_time = time()
        
        # Create original plan
        table_node = TableNode("students")
        selection = SelectionNode([
            Condition("age", "20", Operator.EQ),
            Condition("gpa", "3.5", Operator.GREATER)
        ])
        selection.set_child(table_node)
        project = ProjectNode(["name"])
        project.set_child(selection)
        original_plan = QueryPlan(project)


        # Generate all possible plans
        plans = generate_possible_plans(original_plan, [EquivalenceRules.deconstruct_conjunction])
        

        # Create expected plans
        # Plan 1: Original
        plan1 = original_plan

        # Plan 2: age first, then gpa
        table_node2 = TableNode("students")
        select_gpa = SelectionNode([Condition("gpa", "3.5", Operator.GREATER)])
        select_gpa.set_child(table_node2)
        select_age = SelectionNode([Condition("age", "20", Operator.EQ)])
        select_age.set_child(select_gpa)
        project2 = ProjectNode(["name"])
        project2.set_child(select_age)
        plan2 = QueryPlan(project2)

        # Plan 3: gpa first, then age
        table_node3 = TableNode("students")
        select_age2 = SelectionNode([Condition("age", "20", Operator.EQ)])
        select_age2.set_child(table_node3)
        select_gpa2 = SelectionNode([Condition("gpa", "3.5", Operator.GREATER)])
        select_gpa2.set_child(select_age2)
        project3 = ProjectNode(["name"])
        project3.set_child(select_gpa2)
        plan3 = QueryPlan(project3)

        # Assert number of plans
        assert len(plans) == 3

        # Assert all expected plans exist
        assert any(p == plan1 for p in plans), "Original plan should exist in results"
        assert any(p == plan2 for p in plans), "Age->GPA plan should exist in results"
        assert any(p == plan3 for p in plans), "GPA->Age plan should exist in results"

        print(f"Test two conditions: Passed - {time() - start_time:.6f} s")

    def test_three_conditions(self):
        """Test deconstructing three AND conditions"""
        start_time = time()
        
        # Create original plan
        table_node = TableNode("students")
        selection = SelectionNode([
            Condition("age", "20", Operator.EQ),
            Condition("gpa", "3.5", Operator.GREATER),
            Condition("credits", "60", Operator.GREATER_EQ)
        ])
        selection.set_child(table_node)
        project = ProjectNode(["name"])
        project.set_child(selection)
        original_plan = QueryPlan(project)


        # Generate all possible plans
        plans = generate_possible_plans(original_plan, [EquivalenceRules.deconstruct_conjunction])
    

        # Assert number of plans
        assert len(plans) == 10

        # Create and verify one specific ordering exists
        table_node2 = TableNode("students")
        select_credits = SelectionNode([Condition("credits", "60", Operator.GREATER_EQ)])
        select_credits.set_child(table_node2)
        select_gpa = SelectionNode([Condition("gpa", "3.5", Operator.GREATER)])
        select_gpa.set_child(select_credits)
        select_age = SelectionNode([Condition("age", "20", Operator.EQ)])
        select_age.set_child(select_gpa)
        project2 = ProjectNode(["name"])
        project2.set_child(select_age)
        expected_plan = QueryPlan(project2)

        # Assert expected plan exists
        assert any(p == expected_plan for p in plans), "credits->gpa->age ordering should exist"
        
        # Verify original plan exists
        assert any(p == original_plan for p in plans), "Original plan should exist in results"

        print(f"Test three conditions: Passed - {time() - start_time:.6f} s")

    def test_constants_vs_correlations(self):
        """Test mix of constant comparisons and correlations"""
        start_time = time()
        
        # Create original plan with mix of conditions
        table_node = TableNode("employees")
        selection = SelectionNode([
            Condition("salary", "50000", Operator.GREATER),  # constant
            Condition("dept_id", "dept.id", Operator.EQ),    # correlation
            Condition("age", "30", Operator.GREATER_EQ)      # constant
        ])
        selection.set_child(table_node)
        project = ProjectNode(["name", "salary"])
        project.set_child(selection)
        original_plan = QueryPlan(project)



        plans = generate_possible_plans(original_plan, [EquivalenceRules.deconstruct_conjunction])
        

        # Create one expected ordering: correlation first
        table_node2 = TableNode("employees")
        select_age = SelectionNode([Condition("age", "30", Operator.GREATER_EQ)])
        select_age.set_child(table_node2)
        select_salary = SelectionNode([Condition("salary", "50000", Operator.GREATER)])
        select_salary.set_child(select_age)
        select_dept = SelectionNode([Condition("dept_id", "dept.id", Operator.EQ)])
        select_dept.set_child(select_salary)
        project2 = ProjectNode(["name", "salary"])
        project2.set_child(select_dept)
        expected_plan = QueryPlan(project2)

        assert len(plans) == 10
        assert any(p == original_plan for p in plans), "Original plan should exist"
        assert any(p == expected_plan for p in plans), "Correlation-first plan should exist"

        print(f"Test constants vs correlations: Passed - {time() - start_time:.6f} s")

    def test_complex_predicates(self):
        """Test more complex conditions including ranges"""
        start_time = time()
        
        table_node = TableNode("products")
        selection = SelectionNode([
            Condition("price", "100", Operator.LESS_EQ),     # upper bound
            Condition("quantity", "0", Operator.GREATER),     # lower bound
            Condition("category", "electronics", Operator.EQ) # exact match
        ])
        selection.set_child(table_node)
        project = ProjectNode(["product_id", "name"])
        project.set_child(selection)
        original_plan = QueryPlan(project)


        plans = generate_possible_plans(original_plan, [EquivalenceRules.deconstruct_conjunction])
        

        # Create expected plan with bounds first
        table_node2 = TableNode("products")
        select_category = SelectionNode([Condition("category", "electronics", Operator.EQ)])
        select_category.set_child(table_node2)
        select_quantity = SelectionNode([Condition("quantity", "0", Operator.GREATER)])
        select_quantity.set_child(select_category)
        select_price = SelectionNode([Condition("price", "100", Operator.LESS_EQ)])
        select_price.set_child(select_quantity)
        project2 = ProjectNode(["product_id", "name"])
        project2.set_child(select_price)
        expected_plan = QueryPlan(project2)

        assert len(plans) == 10
        assert any(p == original_plan for p in plans), "Original plan should exist"
        assert any(p == expected_plan for p in plans), "Bounds-first plan should exist"

        print(f"Test complex predicates: Passed - {time() - start_time:.6f} s")

    def test_join_with_selections(self):
        """Test selections on top of a join operation"""
        start_time = time()
        
        # Create base join
        orders = TableNode("orders")
        customers = TableNode("customers")
        join = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("orders.customer_id", "customers.id", Operator.EQ)
        ])
        join.set_children(Pair(orders, customers))
        
        # Add selections on top
        selection = SelectionNode([
            Condition("orders.amount", "1000", Operator.GREATER),
            Condition("customers.status", "VIP", Operator.EQ)
        ])
        selection.set_child(join)
        project = ProjectNode(["orders.id", "customers.name"])
        project.set_child(selection)
        original_plan = QueryPlan(project)


        plans = generate_possible_plans(original_plan, [EquivalenceRules.deconstruct_conjunction])


        # Create expected plan with amount first
        orders2 = TableNode("orders")
        customers2 = TableNode("customers")
        join2 = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("orders.customer_id", "customers.id", Operator.EQ)
        ])
        join2.set_children(Pair(orders2, customers2))
        
        select_status = SelectionNode([Condition("customers.status", "VIP", Operator.EQ)])
        select_status.set_child(join2)
        select_amount = SelectionNode([Condition("orders.amount", "1000", Operator.GREATER)])
        select_amount.set_child(select_status)
        project2 = ProjectNode(["orders.id", "customers.name"])
        project2.set_child(select_amount)
        expected_plan = QueryPlan(project2)

        assert len(plans) == 3  # Original + 2 orderings
        assert any(p == original_plan for p in plans), "Original plan should exist"
        assert any(p == expected_plan for p in plans), "Amount-first plan should exist"

        print(f"Test join with selections: Passed - {time() - start_time:.6f} s")

    def test_multiple_projects(self):
        """Test selections between multiple projections"""
        start_time = time()
        
        table_node = TableNode("employees")
        inner_project = ProjectNode(["id", "name", "salary", "dept"])
        inner_project.set_child(table_node)
        
        selection = SelectionNode([
            Condition("salary", "60000", Operator.GREATER),
            Condition("dept", "IT", Operator.EQ)
        ])
        selection.set_child(inner_project)
        
        outer_project = ProjectNode(["name", "salary"])
        outer_project.set_child(selection)
        original_plan = QueryPlan(outer_project)


        plans = generate_possible_plans(original_plan, [EquivalenceRules.deconstruct_conjunction])
        

        # Create expected plan with salary first
        table_node2 = TableNode("employees")
        inner_project2 = ProjectNode(["id", "name", "salary", "dept"])
        inner_project2.set_child(table_node2)
        
        select_dept = SelectionNode([Condition("dept", "IT", Operator.EQ)])
        select_dept.set_child(inner_project2)
        select_salary = SelectionNode([Condition("salary", "60000", Operator.GREATER)])
        select_salary.set_child(select_dept)
        
        outer_project2 = ProjectNode(["name", "salary"])
        outer_project2.set_child(select_salary)
        expected_plan = QueryPlan(outer_project2)

        assert len(plans) == 3
        assert any(p == original_plan for p in plans), "Original plan should exist"
        assert any(p == expected_plan for p in plans), "Salary-first plan should exist"

        print(f"Test multiple projects: Passed - {time() - start_time:.6f} s")
