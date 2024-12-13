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
        table_node = TableNode("takes")

        # Create a chain of SelectionNodes
        cond_id_course = Condition("id", "course_id", Operator.EQ)
        cond_course_sec = Condition("course_id", "sec_id", Operator.EQ)
        cond_sec_sem = Condition("sec_id", "semester", Operator.EQ)
        cond_sem_year = Condition("semester", "year", Operator.EQ)

        # Build selection chain
        select_sem_year = SelectionNode([cond_sem_year])
        select_sem_year.set_child(table_node)

        select_sec_sem = SelectionNode([cond_sec_sem])
        select_sec_sem.set_child(select_sem_year)

        select_course_sec = SelectionNode([cond_course_sec])
        select_course_sec.set_child(select_sec_sem)

        select_id_course = SelectionNode([cond_id_course])
        select_id_course.set_child(select_course_sec)

        # Create a project node
        project_node = ProjectNode(["id"])
        project_node.set_child(select_id_course)

        # Wrap in a QueryPlan
        plan = QueryPlan(project_node)

        # Apply equivalence rules
        plans = generate_possible_plans(plan, [EquivalenceRules.commute_selections])

        # Validate number of plans (4!)
        assert len(plans) == 24, f"Expected 24 plans, got {len(plans)}"

        print(f"Test 1: Passed - {time() - start_time:.6f} s")

    
    def test_basic_selection_commutativity(self):
        start_time = time()

        # Original plan: σtot_cred>20(σdept_name='CS'(student))
        table_node = TableNode("student")
        select_dept = SelectionNode([Condition("dept_name", "CS", Operator.EQ)])
        select_dept.set_child(table_node)
        select_credits = SelectionNode([Condition("tot_cred", "20", Operator.GREATER)])
        select_credits.set_child(select_dept)
        project = ProjectNode(["name"])
        project.set_child(select_credits)
        original_plan = QueryPlan(project)

        # Generate possible plans
        plans = generate_possible_plans(original_plan, [EquivalenceRules.commute_selections])

        # Create commuted plan: σdept_name='CS'(σtot_cred>20(student))
        table_node2 = TableNode("student")
        select_credits2 = SelectionNode([Condition("tot_cred", "20", Operator.GREATER)])
        select_credits2.set_child(table_node2)
        select_dept2 = SelectionNode([Condition("dept_name", "CS", Operator.EQ)])
        select_dept2.set_child(select_credits2)
        project2 = ProjectNode(["name"])
        project2.set_child(select_dept2)
        expected_plan = QueryPlan(project2)

        # Validate plans
        assert len(plans) == 2, f"Expected 2 plans, got {len(plans)}"
        assert any(p == original_plan for p in plans), "Original plan not found."
        assert any(p == expected_plan for p in plans), "Commuted plan not found."

        print(f"Test basic commutativity: Passed - {time() - start_time:.6f} s")


    def test_three_selections_commutativity(self):
        start_time = time()

        # Original: σcredits>3(σdept_name='CS'(σtitle='AI'(course)))
        table_node = TableNode("course")
        select_title = SelectionNode([Condition("title", "AI", Operator.EQ)])
        select_title.set_child(table_node)
        select_dept = SelectionNode([Condition("dept_name", "CS", Operator.EQ)])
        select_dept.set_child(select_title)
        select_credits = SelectionNode([Condition("credits", "3", Operator.GREATER)])
        select_credits.set_child(select_dept)
        project = ProjectNode(["course_id"])
        project.set_child(select_credits)
        original_plan = QueryPlan(project)

        # Generate possible plans
        plans = generate_possible_plans(original_plan, [EquivalenceRules.commute_selections])

        # Create one expected ordering: σdept_name='CS'(σtitle='AI'(σcredits>3(course)))
        table_node2 = TableNode("course")
        select_credits2 = SelectionNode([Condition("credits", "3", Operator.GREATER)])
        select_credits2.set_child(table_node2)
        select_title2 = SelectionNode([Condition("title", "AI", Operator.EQ)])
        select_title2.set_child(select_credits2)
        select_dept2 = SelectionNode([Condition("dept_name", "CS", Operator.EQ)])
        select_dept2.set_child(select_title2)
        project2 = ProjectNode(["course_id"])
        project2.set_child(select_dept2)
        expected_plan = QueryPlan(project2)

        # Validate plans
        assert len(plans) == 6, f"Expected 6 plans, got {len(plans)}"
        print(f"Test three selections: Passed - {time() - start_time:.6f} s")


    def test_commutativity_with_join(self):
        start_time = time()

        # Create base join: student ⋈ advisor
        student = TableNode("student")
        advisor = TableNode("advisor")
        join = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("student.id", "advisor.s_id", Operator.EQ)
        ])
        join.set_children(Pair(student, advisor))

        # Add selections: σtot_cred>30(σi_id=500(advisor ⋈ student))
        select_i_id = SelectionNode([Condition("advisor.i_id", "500", Operator.EQ)])
        select_i_id.set_child(join)
        select_tot_cred = SelectionNode([Condition("student.tot_cred", "30", Operator.GREATER)])
        select_tot_cred.set_child(select_i_id)
        project = ProjectNode(["student.name", "advisor.i_id"])
        project.set_child(select_tot_cred)
        original_plan = QueryPlan(project)

        # Generate possible plans
        plans = generate_possible_plans(original_plan, [EquivalenceRules.commute_selections])

        # Create commuted plan: σi_id=500(σtot_cred>30(advisor ⋈ student))
        join2 = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("student.id", "advisor.s_id", Operator.EQ)
        ])
        join2.set_children(Pair(TableNode("student"), TableNode("advisor")))
        select_tot_cred2 = SelectionNode([Condition("student.tot_cred", "30", Operator.GREATER)])
        select_tot_cred2.set_child(join2)
        select_i_id2 = SelectionNode([Condition("advisor.i_id", "500", Operator.EQ)])
        select_i_id2.set_child(select_tot_cred2)
        project2 = ProjectNode(["student.name", "advisor.i_id"])
        project2.set_child(select_i_id2)
        expected_plan = QueryPlan(project2)

        # Validate plans
        assert len(plans) == 2, f"Expected 2 plans, got {len(plans)}"
        assert any(p == original_plan for p in plans), "Original plan not found."
        assert any(p == expected_plan for p in plans), "Commuted plan not found."

        print(f"Test join commutativity: Passed - {time() - start_time:.6f} s")

