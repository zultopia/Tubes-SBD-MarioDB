from time import time
from query_plan.query_plan import QueryPlan
from query_plan.nodes.table_node import TableNode
from query_plan.nodes.join_nodes import ConditionalJoinNode, NaturalJoinNode
from query_plan.nodes.selection_node import SelectionNode
from query_plan.nodes.join_nodes import Condition
from query_plan.nodes.project_node import ProjectNode
from query_plan.enums import Operator, JoinAlgorithm
from utils import Pair
from generator import generate_possible_plans
from equivalence_rules import EquivalenceRules

class TestOptimizerRule8:

    def test_course_section_classroom(self):
        """Test join with composite primary keys"""
        start_time = time()
        
        # Original: π(title, building, room_no)(
        #   Course ⋈(course.course_id=section.course_id) 
        #   (Section ⋈(section.building=classroom.building) Classroom)
        # )
        course = TableNode("course")
        section = TableNode("section")
        classroom = TableNode("classroom")
        
        # Join section-classroom
        join1 = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("section.building", "classroom.building", Operator.EQ)
        ])
        join1.set_children(Pair(section, classroom))
        
        # Join with course
        join2 = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("course.course_id", "section.course_id", Operator.EQ)
        ])
        join2.set_children(Pair(course, join1))
        
        project = ProjectNode(["title", "building", "room_no"])
        project.set_child(join2)
        original_plan = QueryPlan(project)

        # Expected: Push projections down keeping join attributes
        course2 = TableNode("course")
        section2 = TableNode("section")
        classroom2 = TableNode("classroom")
        
        project_course = ProjectNode(["title", "course_id"])
        project_course.set_child(course2)
        
        project_section = ProjectNode(["course_id", "building", "room_no"])
        project_section.set_child(section2)
        
        project_classroom = ProjectNode(["building", "room_no"])
        project_classroom.set_child(classroom2)
        
        join1_2 = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("section.building", "classroom.building", Operator.EQ)
        ])
        join1_2.set_children(Pair(project_section, project_classroom))
        
        join2_2 = ConditionalJoinNode(JoinAlgorithm.HASH, [
            Condition("course.course_id", "section.course_id", Operator.EQ)
        ])
        join2_2.set_children(Pair(project_course, join1_2))
        expected_plan = QueryPlan(join2_2)


        plans = generate_possible_plans(original_plan, [
            EquivalenceRules.push_projections_into_join
        ])

        
        assert any(p == expected_plan for p in plans), "Course-Section-Classroom projection distribution should exist"
        print(f"Course-Section-Classroom projection: Passed - {time() - start_time:.6f} s")