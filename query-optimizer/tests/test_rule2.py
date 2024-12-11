from generator import generate_possible_plans
from equivalence_rules import EquivalenceRules

from query_plan.query_plan import QueryPlan
from query_plan.nodes.project_node import ProjectNode
from query_plan.nodes.selection_node import SelectionNode
from query_plan.nodes.selection_node import Condition
from query_plan.nodes.table_node import TableNode
from query_plan.nodes.join_nodes import ConditionalJoinNode, JoinAlgorithm
from query_plan.enums import Operator
from time import time
from utils import Pair

class TestOptimizerRule2:
    def test_1(self):
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
            EquivalenceRules.commute_selections
        ])

        # 4!
        assert len(plans) == 24
        print(f"Test 1: Passed - {time() - start_time:.6f} s") 
    
    def test_basic_selection_commutativity(self):
        """Test basic commutativity of two selections"""
        start_time = time()
        
        # Original plan: σage>20(σsalary>50000(employees))
        table_node = TableNode("employees")
        select_salary = SelectionNode([
            Condition("salary", "50000", Operator.GREATER)
        ])
        select_salary.set_child(table_node)
        select_age = SelectionNode([
            Condition("age", "20", Operator.GREATER)
        ])
        select_age.set_child(select_salary)
        project = ProjectNode(["name"])
        project.set_child(select_age)
        original_plan = QueryPlan(project)


        plans = generate_possible_plans(original_plan, [EquivalenceRules.commute_selections])
        

        # Create expected commuted plan: σsalary>50000(σage>20(employees))
        table_node2 = TableNode("employees")
        select_age2 = SelectionNode([
            Condition("age", "20", Operator.GREATER)
        ])
        select_age2.set_child(table_node2)
        select_salary2 = SelectionNode([
            Condition("salary", "50000", Operator.GREATER)
        ])
        select_salary2.set_child(select_age2)
        project2 = ProjectNode(["name"])
        project2.set_child(select_salary2)
        expected_plan = QueryPlan(project2)

        assert len(plans) == 2  # Original + commuted
        assert any(p == original_plan for p in plans), "Original plan should exist"
        assert any(p == expected_plan for p in plans), "Commuted plan should exist"

        print(f"Test basic commutativity: Passed - {time() - start_time:.6f} s")

    def test_three_selections_commutativity(self):
        """Test commutativity with three selection operations"""
        start_time = time()
        
        # Original: σage>20(σsalary>50000(σdept='IT'(employees)))
        table_node = TableNode("employees")
        select_dept = SelectionNode([
            Condition("dept", "IT", Operator.EQ)
        ])
        select_dept.set_child(table_node)
        select_salary = SelectionNode([
            Condition("salary", "50000", Operator.GREATER)
        ])
        select_salary.set_child(select_dept)
        select_age = SelectionNode([
            Condition("age", "20", Operator.GREATER)
        ])
        select_age.set_child(select_salary)
        project = ProjectNode(["name"])
        project.set_child(select_age)
        original_plan = QueryPlan(project)


        plans = generate_possible_plans(original_plan, [EquivalenceRules.commute_selections])
        

        # Create one expected ordering: σdept='IT'(σage>20(σsalary>50000(employees)))
        table_node2 = TableNode("employees")
        select_salary2 = SelectionNode([
            Condition("salary", "50000", Operator.GREATER)
        ])
        select_salary2.set_child(table_node2)
        select_age2 = SelectionNode([
            Condition("age", "20", Operator.GREATER)
        ])
        select_age2.set_child(select_salary2)
        select_dept2 = SelectionNode([
            Condition("dept", "IT", Operator.EQ)
        ])
        select_dept2.set_child(select_age2)
        project2 = ProjectNode(["name"])
        project2.set_child(select_dept2)
        expected_plan = QueryPlan(project2)

        assert len(plans) == 6  # 3! possible orderings
        assert any(p == original_plan for p in plans), "Original plan should exist"
        assert any(p == expected_plan for p in plans), "Reordered plan should exist"

        print(f"Test three selections: Passed - {time() - start_time:.6f} s")

    def test_commutativity_with_join(self):
        """Test commutativity of selections above a join"""
        start_time = time()
        
        # Create base join
        employees = TableNode("employees")
        departments = TableNode("departments")
        join = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("employees.dept_id", "departments.id", Operator.EQ)
        ])
        join.set_children(Pair(employees, departments))
        
        # Add selections: σage>30(σlocation='NY'(emp ⋈ dept))
        select_location = SelectionNode([
            Condition("departments.location", "NY", Operator.EQ)
        ])
        select_location.set_child(join)
        select_age = SelectionNode([
            Condition("employees.age", "30", Operator.GREATER)
        ])
        select_age.set_child(select_location)
        project = ProjectNode(["employees.name", "departments.name"])
        project.set_child(select_age)
        original_plan = QueryPlan(project)


        plans = generate_possible_plans(original_plan, [EquivalenceRules.commute_selections])
        

        # Create commuted plan: σlocation='NY'(σage>30(emp ⋈ dept))
        employees2 = TableNode("employees")
        departments2 = TableNode("departments")
        join2 = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("employees.dept_id", "departments.id", Operator.EQ)
        ])
        join2.set_children(Pair(employees2, departments2))
        
        select_age2 = SelectionNode([
            Condition("employees.age", "30", Operator.GREATER)
        ])
        select_age2.set_child(join2)
        select_location2 = SelectionNode([
            Condition("departments.location", "NY", Operator.EQ)
        ])
        select_location2.set_child(select_age2)
        project2 = ProjectNode(["employees.name", "departments.name"])
        project2.set_child(select_location2)
        expected_plan = QueryPlan(project2)

        assert len(plans) == 2
        assert any(p == original_plan for p in plans), "Original plan should exist"
        assert any(p == expected_plan for p in plans), "Commuted plan should exist"

        print(f"Test join commutativity: Passed - {time() - start_time:.6f} s")

    def test_mixed_selections_commutativity(self):
        """Test commutativity with different types of conditions"""
        start_time = time()
        
        # Mix of equality, range, and correlation conditions
        table_node = TableNode("employees")
        
        # σdept='IT'(σsalary>mgr.salary(σage>25(employees)))
        select_age = SelectionNode([
            Condition("age", "25", Operator.GREATER)  # Range
        ])
        select_age.set_child(table_node)
        
        select_salary = SelectionNode([
            Condition("salary", "mgr.salary", Operator.GREATER)  # Correlation
        ])
        select_salary.set_child(select_age)
        
        select_dept = SelectionNode([
            Condition("dept", "IT", Operator.EQ)  # Equality
        ])
        select_dept.set_child(select_salary)
        
        project = ProjectNode(["name", "salary"])
        project.set_child(select_dept)
        original_plan = QueryPlan(project)


        plans = generate_possible_plans(original_plan, [EquivalenceRules.commute_selections])
        

        # Create one expected ordering: σage>25(σdept='IT'(σsalary>mgr.salary(employees)))
        table_node2 = TableNode("employees")
        select_salary2 = SelectionNode([
            Condition("salary", "mgr.salary", Operator.GREATER)
        ])
        select_salary2.set_child(table_node2)
        select_dept2 = SelectionNode([
            Condition("dept", "IT", Operator.EQ)
        ])
        select_dept2.set_child(select_salary2)
        select_age2 = SelectionNode([
            Condition("age", "25", Operator.GREATER)
        ])
        select_age2.set_child(select_dept2)
        project2 = ProjectNode(["name", "salary"])
        project2.set_child(select_age2)
        expected_plan = QueryPlan(project2)

        assert len(plans) == 6
        assert any(p == original_plan for p in plans), "Original plan should exist"
        assert any(p == expected_plan for p in plans), "Reordered plan should exist"

        print(f"Test mixed selections: Passed - {time() - start_time:.6f} s")
    