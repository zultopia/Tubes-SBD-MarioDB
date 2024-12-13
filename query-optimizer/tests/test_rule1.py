from data import QOData
from query_plan.nodes.selection_node import SelectionNode
from query_plan.nodes.table_node import TableNode
from query_plan.nodes.project_node import ProjectNode
from query_plan.shared import Condition, Operator
from query_plan.query_plan import QueryPlan
from generator import generate_possible_plans
from equivalence_rules import EquivalenceRules

class TestOptimizerRule1:
    def test_1(self):
        qo_data = QOData.get_instance()

        # Validate basic operations using QOData
        relation = "student"
        assert qo_data.has_relation(relation), f"Relation '{relation}' not found in data."

        # Create the table node
        table_node = TableNode(relation)

        # Create a chain of SelectionNodes using data from QOData
        cond_id_name = Condition("id", "name", Operator.EQ)
        cond_name_dept = Condition("name", "dept_name", Operator.EQ)

        select_name_dept = SelectionNode([cond_name_dept])
        select_name_dept.set_child(table_node)

        select_id_name = SelectionNode([cond_id_name])
        select_id_name.set_child(select_name_dept)

        # Create a project node
        project_node = ProjectNode(["id"])
        project_node.set_child(select_id_name)

        # Wrap in a QueryPlan
        plan = QueryPlan(project_node)

        # Apply equivalence rules
        plans = generate_possible_plans(plan, [EquivalenceRules.deconstruct_conjunction])
        assert len(plans) == 1, "Only one plan expected."

        print(f"Test 1: Passed")

    def test_2(self):
        qo_data = QOData.get_instance()
        relation = "course"
        attribute = "course_id"
        
        # Check existence of relation and attribute
        assert qo_data.has_relation(relation), f"Relation '{relation}' not found."
        assert qo_data.has_attribute(attribute, relation), f"Attribute '{attribute}' not found in '{relation}'."

        # Create the table node
        table_node = TableNode(relation)

        # Create selection nodes for conditions derived from MOCK_DATA
        cond_course_title = Condition(attribute, "title", Operator.EQ)
        cond_title_dept = Condition("title", "dept_name", Operator.EQ)

        selection = SelectionNode([cond_course_title, cond_title_dept])
        selection.set_child(table_node)

        # Create a project node
        project_node = ProjectNode(["course_id"])
        project_node.set_child(selection)

        # Wrap in a QueryPlan
        plan = QueryPlan(project_node)

        # Apply equivalence rules
        plans = generate_possible_plans(plan, [EquivalenceRules.deconstruct_conjunction])
        assert len(plans) == 3, "Expected three plans with deconstructed conjunctions."

        print(f"Test 2: Passed")

    def test_three_conditions(self):
        qo_data = QOData.get_instance()
        relation = "takes"
        
        # Validate relation and attributes
        assert qo_data.has_relation(relation), f"Relation '{relation}' not found."
        assert all(
            qo_data.has_attribute(attr, relation) for attr in ["id", "course_id", "sec_id"]
        ), "Some attributes are missing in 'takes'."

        # Create table node and selection conditions
        table_node = TableNode(relation)
        cond_id_course = Condition("id", "course_id", Operator.EQ)
        cond_course_sec = Condition("course_id", "sec_id", Operator.EQ)
        cond_sec_grade = Condition("sec_id", "grade", Operator.EQ)

        selection = SelectionNode([cond_id_course, cond_course_sec, cond_sec_grade])
        selection.set_child(table_node)

        # Project and create plan
        project_node = ProjectNode(["id"])
        project_node.set_child(selection)
        original_plan = QueryPlan(project_node)

        # Apply equivalence rules
        plans = generate_possible_plans(original_plan, [EquivalenceRules.deconstruct_conjunction])
        assert len(plans) == 10, "Expected ten possible plans for three conditions."

        # Create an expected plan structure with a specific condition order
        table_node2 = TableNode(relation)
        select_grade = SelectionNode([cond_sec_grade])
        select_grade.set_child(table_node2)

        select_course_sec = SelectionNode([cond_course_sec])
        select_course_sec.set_child(select_grade)

        select_id_course = SelectionNode([cond_id_course])
        select_id_course.set_child(select_course_sec)

        project_node2 = ProjectNode(["id"])
        project_node2.set_child(select_id_course)

        expected_plan = QueryPlan(project_node2)

        # Verify the expected plan exists in the generated plans
        assert any(plan == expected_plan for plan in plans), "Expected condition order not found in generated plans."

        print(f"Test three conditions: Passed")

    def test_complex_predicates(self):
        qo_data = QOData.get_instance()
        relation = "section"

        # Validate relation and attributes
        assert qo_data.has_relation(relation), f"Relation '{relation}' not found."
        assert all(
            qo_data.has_attribute(attr, relation) for attr in ["course_id", "sec_id", "semester"]
        ), "Some attributes are missing in 'section'."

        # Create table node and conditions
        table_node = TableNode(relation)
        cond_course_sec = Condition("course_id", "sec_id", Operator.EQ)
        cond_sec_sem = Condition("sec_id", "semester", Operator.EQ)
        cond_sem_year = Condition("semester", "year", Operator.EQ)

        selection = SelectionNode([cond_course_sec, cond_sec_sem, cond_sem_year])
        selection.set_child(table_node)

        # Create project node and query plan
        project_node = ProjectNode(["course_id"])
        project_node.set_child(selection)
        plan = QueryPlan(project_node)

        # Apply equivalence rules
        plans = generate_possible_plans(plan, [EquivalenceRules.deconstruct_conjunction])
        assert len(plans) == 10, "Expected six possible plans for three conditions."

        print(f"Test complex predicates: Passed")
