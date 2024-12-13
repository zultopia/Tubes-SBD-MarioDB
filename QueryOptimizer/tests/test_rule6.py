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

class TestOptimizerRule6:
    def test_natural_join_associativity(self):
        """Test Rule 6a: Natural Join Associativity"""
        start_time = time()
        
        # Original: (Student ⋈ Enrollment) ⋈ Course
        student = TableNode("Student")
        enrollment = TableNode("Enrollment")
        course = TableNode("Course")

        inner_join = NaturalJoinNode(JoinAlgorithm.HASH)
        inner_join.set_children(Pair(student, enrollment))

        outer_join = NaturalJoinNode(JoinAlgorithm.HASH)
        outer_join.set_children(Pair(inner_join, course))
        original_plan = QueryPlan(outer_join)

        # Expected: Student ⋈ (Enrollment ⋈ Course)
        student2 = TableNode("Student")
        enrollment2 = TableNode("Enrollment")
        course2 = TableNode("Course")

        inner_join2 = NaturalJoinNode(JoinAlgorithm.HASH)
        inner_join2.set_children(Pair(enrollment2, course2))

        outer_join2 = NaturalJoinNode(JoinAlgorithm.HASH)
        outer_join2.set_children(Pair(student2, inner_join2))
        expected_plan = QueryPlan(outer_join2)

        plans = generate_possible_plans(original_plan, [
            EquivalenceRules.associativeJoins
        ])
        assert any(p == expected_plan for p in plans), "Natural join association should exist"
        print(f"Natural join associativity: Passed - {time() - start_time:.6f} s")

    def test_theta_join_associativity(self):
        """Test Rule 6b: Theta Join Associativity"""
        start_time = time()
        
        # Original: (advisor ⋈(s_id=id) student) ⋈(dept_name=dept_name) course
        advisor = TableNode("advisor")
        student = TableNode("student")
        course = TableNode("course")

        # θ1: advisor ⋈(s_id=id) student
        inner_join = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("s_id", "id", Operator.EQ)
        ])
        inner_join.set_children(Pair(advisor, student))

        # θ12: (advisor ⋈ student) ⋈(dept_name=dept_name) course
        outer_join = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("dept_name", "dept_name", Operator.EQ)
        ])
        outer_join.set_children(Pair(inner_join, course))
        original_plan = QueryPlan(outer_join)

        # Expected: advisor ⋈(s_id=id) (student ⋈(dept_name=dept_name) course)
        advisor2 = TableNode("advisor")
        student2 = TableNode("student")
        course2 = TableNode("course")

        # θ2: student ⋈(dept_name=dept_name) course
        inner_join2 = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("dept_name", "dept_name", Operator.EQ)
        ])
        inner_join2.set_children(Pair(student2, course2))

        # θ1: advisor ⋈(s_id=id) (student ⋈ course)
        outer_join2 = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("s_id", "id", Operator.EQ)
        ])
        outer_join2.set_children(Pair(advisor2, inner_join2))
        expected_plan = QueryPlan(outer_join2)

        plans = generate_possible_plans(original_plan, [
            EquivalenceRules.associativeJoins
        ])
        
        assert any(p == expected_plan for p in plans), "Theta join association should exist"
        print(f"Theta join associativity: Passed - {time() - start_time:.6f} s")


    def test_complex_conditions(self):
        """Test associativity with complex join conditions"""
        start_time = time()
        
        # Original: (takes ⋈(grade>2) course) ⋈(dept_name=dept_name AND credits>3) instructor
        takes = TableNode("takes")
        course = TableNode("course")
        instructor = TableNode("instructor")

        # First join
        inner_join = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("grade", "2", Operator.GREATER)
        ])
        inner_join.set_children(Pair(takes, course))

        # Second join with multiple conditions
        outer_join = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("dept_name", "dept_name", Operator.EQ),
            Condition("credits", "3", Operator.GREATER)
        ])
        outer_join.set_children(Pair(inner_join, instructor))
        original_plan = QueryPlan(outer_join)

        # Expected: takes ⋈(grade>2) (course ⋈(dept_name=dept_name AND credits>3) instructor)
        takes2 = TableNode("takes")
        course2 = TableNode("course")
        instructor2 = TableNode("instructor")

        # Join between course and instructor first
        inner_join2 = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("dept_name", "dept_name", Operator.EQ),
            Condition("credits", "3", Operator.GREATER)
        ])
        inner_join2.set_children(Pair(course2, instructor2))

        # Then join with takes
        outer_join2 = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("grade", "2", Operator.GREATER)
        ])
        outer_join2.set_children(Pair(takes2, inner_join2))
        expected_plan = QueryPlan(outer_join2)

        plans = generate_possible_plans(original_plan, [
            EquivalenceRules.associativeJoins
        ])
        # print(original_plan)
        # print(expected_plan)
        # print(plans)
        
        assert any(p == expected_plan for p in plans), "Complex condition association should exist"
        print(f"Complex conditions: Passed - {time() - start_time:.6f} s")
