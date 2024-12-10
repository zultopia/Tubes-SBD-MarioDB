# test_query_optimizer.py
import unittest
from query_optimizer import get_parse_tree
from from_parse_tree import from_parse_tree
from generator import generate_possible_plans
from query_plan.query_plan import QueryPlan

class TestQueryOptimizer(unittest.TestCase):
    def test_project_node_preservation(self):
        """
        Test that all generated plans include the PROJECT node.
        """
        query_string = "SELECT a FROM b WHERE a = b AND b = c;"
        tree = get_parse_tree(query_string)
        query_plan = from_parse_tree(tree)
        plans = generate_possible_plans(query_plan)

        for plan in plans:
            serialized = plan.serialize()
            self.assertIn("PROJECT[a]", serialized, f"PROJECT node missing in plan: {serialized}")

    def test_deconstruct_conjunction_creates_correct_structures(self):
        """
        Test that the deconstruct_conjunction rule creates the correct parent-child structures.
        """
        query_string = "SELECT a FROM b WHERE a = b AND b = c;"
        tree = get_parse_tree(query_string)
        query_plan = from_parse_tree(tree)
        plans = generate_possible_plans(query_plan)

        expected_serializations = [
            "PROJECT[a]->SELECT[a = b]->SELECT[b = c]->TABLE[b AS b]",
            "PROJECT[a]->SELECT[b = c]->SELECT[a = b]->TABLE[b AS b]",
            "PROJECT[a]->SELECT[a = b, b = c]->TABLE[b AS b]"
        ]

        for expected in expected_serializations:
            exists = any(expected in plan.serialize() for plan in plans)
            self.assertTrue(exists, f"Expected serialization '{expected}' not found in any plan.")

    def test_no_extra_nodes_present(self):
        """
        Ensure that no extra nodes are present in the transformed plans.
        """
        query_string = "SELECT a FROM b WHERE a = b AND b = c;"
        tree = get_parse_tree(query_string)
        query_plan = from_parse_tree(tree)
        plans = generate_possible_plans(query_plan)

        allowed_substrings = [
            "PROJECT[a]",
            "SELECT[a = b]",
            "SELECT[b = c]",
            "SELECT[a = b, b = c]",
            "TABLE[b AS b]"
        ]

        for plan in plans:
            serialized = plan.serialize()
            nodes = serialized.split("->")
            for node in nodes:
                self.assertIn(node, allowed_substrings, f"Unexpected node '{node}' found in plan: {serialized}")

if __name__ == '__main__':
    unittest.main()
