from time import time
from QueryOptimizer.query_plan.query_plan import QueryPlan
from QueryOptimizer.query_plan.nodes.table_node import TableNode
from QueryOptimizer.query_plan.nodes.join_nodes import ConditionalJoinNode, NaturalJoinNode
from QueryOptimizer.query_plan.nodes.selection_node import SelectionNode
from QueryOptimizer.query_plan.nodes.join_nodes import Condition
from QueryOptimizer.query_plan.enums import Operator, JoinAlgorithm
from QueryOptimizer.utils import Pair
from QueryOptimizer.generator import generate_possible_plans
from QueryOptimizer.equivalence_rules import EquivalenceRules

class TestOptimizerRule5:
    def test_simple_join_commutativity(self):
        """Basic theta join commutativity"""
        start_time = time()
        
        # Original: Employee ⋈(emp.dept_id = dept.id) Department
        emp_table = TableNode("Employee")
        dept_table = TableNode("Department")
        join = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("emp.dept_id", "dept.id", Operator.EQ)
        ])
        join.set_children(Pair(emp_table, dept_table))
        original_plan = QueryPlan(join)

        # Expected: Department ⋈(emp.dept_id = dept.id) Employee
        emp_table2 = TableNode("Employee")
        dept_table2 = TableNode("Department")
        join2 = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("emp.dept_id", "dept.id", Operator.EQ)
        ])
        join2.set_children(Pair(dept_table2, emp_table2))
        expected_plan = QueryPlan(join2)

        plans = generate_possible_plans(original_plan, [
            EquivalenceRules.switchChildrenJoin
        ])
        
        assert any(p == expected_plan for p in plans), "Simple join commutation should exist"
        print(f"Simple join commutativity: Passed - {time() - start_time:.6f} s")

    def test_brutal_multiple_joins(self):
        """Multiple joins with complex conditions"""
        start_time = time()
        
        # Original: (Emp ⋈ Dept) ⋈ Project
        emp_table = TableNode("Employee")
        dept_table = TableNode("Department")
        proj_table = TableNode("Project")

        # First join: Emp ⋈ Dept
        inner_join = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("emp.dept_id", "dept.id", Operator.EQ),
            Condition("emp.salary", "dept.avg_salary", Operator.GREATER)
        ])
        inner_join.set_children(Pair(emp_table, dept_table))

        # Outer join: (Emp ⋈ Dept) ⋈ Project
        outer_join = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("emp.id", "project.emp_id", Operator.EQ),
            Condition("dept.budget", "project.budget", Operator.GREATER_EQ)
        ])
        outer_join.set_children(Pair(inner_join, proj_table))
        original_plan = QueryPlan(outer_join)

        # One expected variation: Project ⋈ (Dept ⋈ Emp)
        emp_table2 = TableNode("Employee")
        dept_table2 = TableNode("Department")
        proj_table2 = TableNode("Project")

        inner_join2 = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("emp.dept_id", "dept.id", Operator.EQ),
            Condition("emp.salary", "dept.avg_salary", Operator.GREATER)
        ])
        inner_join2.set_children(Pair(dept_table2, emp_table2))

        outer_join2 = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("emp.id", "project.emp_id", Operator.EQ),
            Condition("dept.budget", "project.budget", Operator.GREATER_EQ)
        ])
        outer_join2.set_children(Pair(proj_table2, inner_join2))
        expected_plan = QueryPlan(outer_join2)

        plans = generate_possible_plans(original_plan, [
            EquivalenceRules.switchChildrenJoin
        ])
        
        assert any(p == expected_plan for p in plans), "Complex join commutation should exist"
        print(f"Brutal multiple joins: Passed - {time() - start_time:.6f} s")

    def test_natural_join_commutativity(self):
        """Natural joins with multiple shared attributes"""
        start_time = time()
        
        # Original: Student ⋈ Enrollment ⋈ Course
        student = TableNode("Student")
        enrollment = TableNode("Enrollment")
        course = TableNode("Course")

        inner_join = NaturalJoinNode(JoinAlgorithm.HASH)
        inner_join.set_children(Pair(student, enrollment))

        outer_join = NaturalJoinNode(JoinAlgorithm.HASH)
        outer_join.set_children(Pair(inner_join, course))
        original_plan = QueryPlan(outer_join)

        # Expected: Course ⋈ (Enrollment ⋈ Student)
        student2 = TableNode("Student")
        enrollment2 = TableNode("Enrollment")
        course2 = TableNode("Course")

        inner_join2 = NaturalJoinNode(JoinAlgorithm.HASH)
        inner_join2.set_children(Pair(enrollment2, student2))

        outer_join2 = NaturalJoinNode(JoinAlgorithm.HASH)
        outer_join2.set_children(Pair(course2, inner_join2))
        expected_plan = QueryPlan(outer_join2)

        plans = generate_possible_plans(original_plan, [
            EquivalenceRules.switchChildrenJoin
        ])
        
        assert any(p == expected_plan for p in plans), "Natural join commutation should exist"
        print(f"Natural join commutativity: Passed - {time() - start_time:.6f} s")

    def test_mixed_join_types(self):
        """Mix of natural and theta joins with complex conditions"""
        start_time = time()
        
        # Original: (Emp ⋈ Dept) ⋈(natural) Project
        emp_table = TableNode("Employee")
        dept_table = TableNode("Department")
        proj_table = TableNode("Project")

        # Theta join
        theta_join = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("emp.dept_id", "dept.id", Operator.EQ),
            Condition("emp.level", "dept.min_level", Operator.GREATER_EQ)
        ])
        theta_join.set_children(Pair(emp_table, dept_table))

        # Natural join on top
        natural_join = NaturalJoinNode(JoinAlgorithm.HASH)
        natural_join.set_children(Pair(theta_join, proj_table))
        original_plan = QueryPlan(natural_join)

        # Expected: Project ⋈(natural) (Dept ⋈ Emp)
        emp_table2 = TableNode("Employee")
        dept_table2 = TableNode("Department")
        proj_table2 = TableNode("Project")

        theta_join2 = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("emp.dept_id", "dept.id", Operator.EQ),
            Condition("emp.level", "dept.min_level", Operator.GREATER_EQ)
        ])
        theta_join2.set_children(Pair(dept_table2, emp_table2))

        natural_join2 = NaturalJoinNode(JoinAlgorithm.HASH)
        natural_join2.set_children(Pair(proj_table2, theta_join2))
        expected_plan = QueryPlan(natural_join2)

        plans = generate_possible_plans(original_plan, [
            EquivalenceRules.switchChildrenJoin
        ])
        
        assert any(p == expected_plan for p in plans), "Mixed join commutation should exist"
        print(f"Mixed join types: Passed - {time() - start_time:.6f} s")

    def test_joins_with_selections(self):
        """Joins with selections and multiple conditions"""
        start_time = time()
        
        # Original: σ(budget>1000)(Dept ⋈(emp.salary>dept.avg) Emp)
        emp_table = TableNode("Employee")
        dept_table = TableNode("Department")
        
        join = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("emp.salary", "dept.avg", Operator.GREATER)
        ])
        join.set_children(Pair(dept_table, emp_table))
        
        selection = SelectionNode([
            Condition("budget", "1000", Operator.GREATER)
        ])
        selection.set_child(join)
        original_plan = QueryPlan(selection)

        # Expected: σ(budget>1000)(Emp ⋈(emp.salary>dept.avg) Dept)
        emp_table2 = TableNode("Employee")
        dept_table2 = TableNode("Department")
        
        join2 = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("emp.salary", "dept.avg", Operator.GREATER)
        ])
        join2.set_children(Pair(emp_table2, dept_table2))
        
        selection2 = SelectionNode([
            Condition("budget", "1000", Operator.GREATER)
        ])
        selection2.set_child(join2)
        expected_plan = QueryPlan(selection2)

        plans = generate_possible_plans(original_plan, [
            EquivalenceRules.switchChildrenJoin
        ])
        
        assert any(p == expected_plan for p in plans), "Join with selection commutation should exist"
        print(f"Joins with selections: Passed - {time() - start_time:.6f} s")