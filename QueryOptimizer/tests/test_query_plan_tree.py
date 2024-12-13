from QueryOptimizer.query_plan.query_plan import QueryPlan
from QueryOptimizer.query_plan.nodes.table_node import TableNode
from QueryOptimizer.query_plan.nodes.project_node import ProjectNode
from QueryOptimizer.query_plan.nodes.join_nodes import ConditionalJoinNode, NaturalJoinNode, JoinNode
from QueryOptimizer.query_plan.nodes.update_node import UpdateNode
from QueryOptimizer.query_plan.nodes.selection_node import SelectionNode
from QueryOptimizer.query_plan.nodes.sorting_node import SortingNode
from QueryOptimizer.query_plan.nodes.limit_node import LimitNode
from QueryOptimizer.query_plan.enums import JoinAlgorithm, Operator
from QueryOptimizer.query_plan.shared import Condition
from QueryOptimizer.utils import Pair

from QueryOptimizer.query_optimizer import get_parse_tree
from QueryOptimizer.from_parse_tree import from_parse_tree
from time import time
from QueryOptimizer.data import QOData


class TestQueryPlanGeneration():

    def assertQueryPlansEqual(self, plan1: QueryPlan, plan2: QueryPlan):
        """
        Custom assertion to compare two QueryPlans.
        """
        assert plan1 == plan2, f"Plans do not match.\nPlan1: {plan1}\nPlan2: {plan2}"

    def test_1(self):
        """Test simple SELECT with one attribute."""
        start_time = time()

        query_string = "SELECT id FROM student;"

        # Parse the query string into a QueryPlan
        parse_tree = get_parse_tree(query_string)
        parsed_plan = from_parse_tree(parse_tree)

        # Manually construct the expected QueryPlan
        attributes = QOData.get_instance().get_all_attributes("student")
        table_node = TableNode("student")
        project_node = ProjectNode(["student.id"])
        project_node.set_child(table_node)
        expected_plan = QueryPlan(project_node)

        # Assert equality
        self.assertQueryPlansEqual(parsed_plan, expected_plan)

        print(f"Test 1: Passed - {time() - start_time:.6f} s")

    def test_2(self):
        """Test SELECT * with aliasing and JOIN."""
        start_time = time()

        query_string = "SELECT * FROM student AS s JOIN instructor AS l ON s.id = l.id;"

        # Parse the query string into a QueryPlan
        parse_tree = get_parse_tree(query_string)
        parsed_plan = from_parse_tree(parse_tree)

        # Manually construct the expected QueryPlan
        student_attributes = QOData.get_instance().get_all_attributes("student")
        lecturer_attributes = QOData.get_instance().get_all_attributes("instructor")


        table_s = TableNode("student", alias="s")
        table_l = TableNode("instructor", alias="l")
        join_conditions = [Condition("s.id", "l.id", Operator.EQ)]
        join_node = ConditionalJoinNode(JoinAlgorithm.HASH, join_conditions)
        join_node.set_children(Pair(table_s, table_l))
        project_node = ProjectNode(['*'])
        project_node.set_child(join_node)
        expected_plan = QueryPlan(project_node)


        # Assert equality
        self.assertQueryPlansEqual(parsed_plan, expected_plan)

        print(f"Test 2: Passed - {time() - start_time:.6f} s")

    def test_3(self):
        """Test UPDATE statement with one condition."""
        start_time = time()

        query_string = "UPDATE student SET dept_name = 'CS' WHERE id = 1;"

        # Parse the query string into a QueryPlan
        parse_tree = get_parse_tree(query_string)
        parsed_plan = from_parse_tree(parse_tree)

        # Manually construct the expected QueryPlan
        attributes = QOData.get_instance().get_all_attributes("student")
        table_node = TableNode("student")
        condition = Condition("id", "1", Operator.EQ)
        selection_node = SelectionNode([condition])
        selection_node.set_child(table_node)
        update_node = UpdateNode([("dept_name", "'CS'")])
        update_node.set_child(selection_node)
        expected_plan = QueryPlan(update_node)

        # Assert equality
        self.assertQueryPlansEqual(parsed_plan, expected_plan)

        print(f"Test 3: Passed - {time() - start_time:.6f} s")