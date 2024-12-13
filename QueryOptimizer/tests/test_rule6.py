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
        
        # Original: (Orders ⋈(o.cust_id=c.id) Customer) ⋈(c.region=s.region) Shipper
        orders = TableNode("Orders")
        customer = TableNode("Customer")
        shipper = TableNode("Shipper")

        # θ1: Orders ⋈(o.cust_id=c.id) Customer
        inner_join = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("o.cust_id", "c.id", Operator.EQ)
        ])
        inner_join.set_children(Pair(orders, customer))

        # θ12: (Orders ⋈ Customer) ⋈(c.region=s.region) Shipper 
        outer_join = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("c.region", "s.region", Operator.EQ)
        ])
        outer_join.set_children(Pair(inner_join, shipper))
        original_plan = QueryPlan(outer_join)

        # Expected: Orders ⋈(o.cust_id=c.id) (Customer ⋈(c.region=s.region) Shipper)
        orders2 = TableNode("Orders")
        customer2 = TableNode("Customer")
        shipper2 = TableNode("Shipper")

        # θ2: Customer ⋈(c.region=s.region) Shipper
        inner_join2 = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("c.region", "s.region", Operator.EQ)
        ])
        inner_join2.set_children(Pair(customer2, shipper2))

        # θ1: Orders ⋈(o.cust_id=c.id) (Customer ⋈ Shipper)
        outer_join2 = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("o.cust_id", "c.id", Operator.EQ)
        ])
        outer_join2.set_children(Pair(orders2, inner_join2))
        expected_plan = QueryPlan(outer_join2)

        plans = generate_possible_plans(original_plan, [
            EquivalenceRules.associativeJoins
        ])
        
        assert any(p == expected_plan for p in plans), "Theta join association should exist"
        print(f"Theta join associativity: Passed - {time() - start_time:.6f} s")


    def test_complex_conditions(self):
        """Test associativity with complex join conditions"""
        start_time = time()
        
        # Original: (Orders ⋈(o.amount>p.min_amount) Payment) ⋈(p.status='verified' AND p.type=s.type) Status
        orders = TableNode("Orders")
        payment = TableNode("Payment")
        status = TableNode("Status")

        # First join
        inner_join = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("o.amount", "p.min_amount", Operator.GREATER)
        ])
        inner_join.set_children(Pair(orders, payment))

        # Second join with multiple conditions
        outer_join = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("p.status", "verified", Operator.EQ),
            Condition("p.type", "s.type", Operator.EQ)
        ])
        outer_join.set_children(Pair(inner_join, status))
        original_plan = QueryPlan(outer_join)

        # Expected: Orders ⋈(o.amount>p.min_amount) (Payment ⋈(p.status='verified' AND p.type=s.type) Status)
        orders2 = TableNode("Orders")
        payment2 = TableNode("Payment")
        status2 = TableNode("Status")

        # Join between Payment and Status first
        inner_join2 = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("p.status", "verified", Operator.EQ),
            Condition("p.type", "s.type", Operator.EQ)
        ])
        inner_join2.set_children(Pair(payment2, status2))

        # Then join with Orders
        outer_join2 = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("o.amount", "p.min_amount", Operator.GREATER)
        ])
        outer_join2.set_children(Pair(orders2, inner_join2))
        expected_plan = QueryPlan(outer_join2)

        plans = generate_possible_plans(original_plan, [
            EquivalenceRules.associativeJoins
        ])
        print(original_plan)
        print(expected_plan)
        print(plans)
        
        assert any(p == expected_plan for p in plans), "Complex condition association should exist"
        print(f"Complex conditions: Passed - {time() - start_time:.6f} s")

