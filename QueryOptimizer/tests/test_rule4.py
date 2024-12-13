from time import time
from QueryOptimizer.query_plan.query_plan import QueryPlan
from QueryOptimizer.query_plan.nodes.table_node import TableNode
from QueryOptimizer.query_plan.nodes.join_nodes import ConditionalJoinNode
from QueryOptimizer.query_plan.nodes.selection_node import SelectionNode
from QueryOptimizer.query_plan.nodes.join_nodes import Condition
from QueryOptimizer.query_plan.enums import Operator, JoinAlgorithm
from QueryOptimizer.utils import Pair
from QueryOptimizer.generator import generate_possible_plans
from QueryOptimizer.equivalence_rules import EquivalenceRules

class TestOptimizerRule4:
    def test_associative_joins_1(self):
        """Test combining selections with both cartesian product and theta joins"""
        start_time = time()
        
        # Create original plan: σ(grade='A')(σ(student_id=id)(Student × Course))
        student_table = TableNode("Student")
        course_table = TableNode("Course")
        
        # First cartesian product
        cart_join = ConditionalJoinNode(JoinAlgorithm.NESTED_LOOP, [])
        cart_join.set_children(Pair(student_table, course_table))
        
        # Add first selection (will become join condition)
        selection1 = SelectionNode([
            Condition("student_id", "id", Operator.EQ)
        ])
        selection1.set_child(cart_join)
        
        # Add second selection
        selection2 = SelectionNode([
            Condition("grade", "A", Operator.EQ)
        ])
        selection2.set_child(selection1)
        
        original_plan = QueryPlan(selection2)


        # Expected plan: Student ⋈(student_id=id AND grade='A') Course
        student_table2 = TableNode("Student")
        course_table2 = TableNode("Course")
        combined_join = ConditionalJoinNode(JoinAlgorithm.NESTED_LOOP, [
            Condition("student_id", "id", Operator.EQ),
            Condition("grade", "A", Operator.EQ)
        ])
        combined_join.set_children(Pair(student_table2, course_table2))
        expected_plan = QueryPlan(combined_join)

        plans = generate_possible_plans(original_plan, [
            EquivalenceRules.combineJoinCondition
        ])
        
        assert any(p == expected_plan for p in plans), "Combine Join transformation should exist"
        print(f"Combine Joins test 1: Passed - {time() - start_time:.6f} s")

    def test_associative_joins_2(self):
        """Test more complex combination of selections and joins"""
        start_time = time()
        
        # Create original plan: σ(salary>manager_salary)(σ(dept=dept_id)(Employee × Department))
        emp_table = TableNode("Employee")
        dept_table = TableNode("Department")
        
        # Start with cartesian product
        cart_join = ConditionalJoinNode(JoinAlgorithm.NESTED_LOOP, [])
        cart_join.set_children(Pair(emp_table, dept_table))
        
        # Add dept join condition as selection
        selection1 = SelectionNode([
            Condition("dept", "dept_id", Operator.EQ)
        ])
        selection1.set_child(cart_join)
        
        # Add salary comparison as selection
        selection2 = SelectionNode([
            Condition("salary", "manager_salary", Operator.GREATER)
        ])
        selection2.set_child(selection1)
        
        original_plan = QueryPlan(selection2)

        # Expected plan: Employee ⋈(dept=dept_id AND salary>manager_salary) Department
        emp_table2 = TableNode("Employee")
        dept_table2 = TableNode("Department")
        combined_join = ConditionalJoinNode(JoinAlgorithm.NESTED_LOOP, [
            Condition("dept", "dept_id", Operator.EQ),
            Condition("salary", "manager_salary", Operator.GREATER)
        ])
        combined_join.set_children(Pair(emp_table2, dept_table2))
        expected_plan = QueryPlan(combined_join)

        plans = generate_possible_plans(original_plan, [
            EquivalenceRules.combineJoinCondition
        ])
        
        assert any(p == expected_plan for p in plans), "Complex Combine Join transformation should exist"
        print(f"Combine Joins test 2: Passed - {time() - start_time:.6f} s")