from time import time
from query_plan.query_plan import QueryPlan
from query_plan.nodes.table_node import TableNode
from query_plan.nodes.join_nodes import ConditionalJoinNode, NaturalJoinNode
from query_plan.nodes.selection_node import SelectionNode
from query_plan.nodes.join_nodes import Condition
from query_plan.enums import Operator, JoinAlgorithm
from utils import Pair
from generator import generate_possible_plans
from equivalence_rules import EquivalenceRules


class TestOptimizerRule7:
    def test_selection_distribution_conditional_join(self):
        """Test Rule 7a: Selection Distribution for Conditional Join"""
        start_time = time()

        # Original: σ(student.gpa > 3.5 AND instructor.salary < 70000) (Student ⋉(id = i_id) Instructor)
        student = TableNode("student")
        instructor = TableNode("instructor")
        join_node = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("student.id", "instructor.i_id", Operator.EQ)
        ])
        join_node.set_children(Pair(student, instructor))

        selection_conditions = [
            Condition("student.tot_cred", "3.5", Operator.GREATER),
            Condition("instructor.salary", "70000", Operator.LESS)
        ]
        selection_node = SelectionNode(selection_conditions)
        selection_node.set_child(join_node)
        original_plan = QueryPlan(selection_node)

        # Expected: (σ(student.gpa > 3.5) Student) ⋉(id = i_id) (σ(instructor.salary < 70000) Instructor)
        student2 = TableNode("student")
        instructor2 = TableNode("instructor")
        left_selection = SelectionNode([Condition("student.tot_cred", "3.5", Operator.GREATER)])
        left_selection.set_child(student2)
        right_selection = SelectionNode([Condition("instructor.salary", "70000", Operator.LESS)])
        right_selection.set_child(instructor2)
        expected_join = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("student.id", "instructor.i_id", Operator.EQ)
        ])
        expected_join.set_children(Pair(left_selection, right_selection))
        expected_plan = QueryPlan(expected_join)
        # Apply the rule
        plans = generate_possible_plans(original_plan, [
            EquivalenceRules.distributeSelection
        ])


        # Verify the expected plan is in the generated plans
        assert any(p == expected_plan for p in plans), "Selection distribution over Conditional Join should exist"
        print(f"Selection distribution over Conditional Join: Passed - {time() - start_time:.6f} s")

    def test_selection_distribution_natural_join(self):
        """Test Rule 7b: Selection Distribution for Natural Join"""
        start_time = time()

        # Original: σ(takes.grade > 'B' AND course.credits < 3) (Takes ⋉ Course)
        takes = TableNode("takes")
        course = TableNode("course")
        join_node = NaturalJoinNode(JoinAlgorithm.HASH)
        join_node.set_children(Pair(takes, course))

        selection_conditions = [
            Condition("takes.grade", "B", Operator.GREATER),
            Condition("course.credits", "3", Operator.LESS)
        ]
        selection_node = SelectionNode(selection_conditions)
        selection_node.set_child(join_node)
        original_plan = QueryPlan(selection_node)

        # Expected: (σ(takes.grade > 'B') Takes) ⋉ (σ(course.credits < 3) Course)
        takes2 = TableNode("takes")
        course2 = TableNode("course")
        left_selection = SelectionNode([Condition("takes.grade", "B", Operator.GREATER)])
        left_selection.set_child(takes2)
        right_selection = SelectionNode([Condition("course.credits", "3", Operator.LESS)])
        right_selection.set_child(course2)
        expected_join = NaturalJoinNode(JoinAlgorithm.HASH)
        expected_join.set_children(Pair(left_selection, right_selection))
        expected_plan = QueryPlan(expected_join)

        # Apply the rule
        plans = generate_possible_plans(original_plan, [
            EquivalenceRules.distributeSelection
        ])

        # Verify the expected plan is in the generated plans
        assert any(p == expected_plan for p in plans), "Selection distribution over Natural Join should exist"
        print(f"Selection distribution over Natural Join: Passed - {time() - start_time:.6f} s")