from equivalence_rules import EquivalenceRules
from query_plan.query_plan import (
    QueryPlan, QueryNode, ProjectNode, UnionSelectionNode, SelectionNode,
    JoinNode, ConditionalJoinNode, NaturalJoinNode, SortingNode, TableNode, Pair, LimitNode
)
from typing import List
from collections import deque

def generate_possible_plans(query: 'QueryPlan', rules: List[EquivalenceRules] = [
    EquivalenceRules.deconstruct_conjunction, # Rule 1
    EquivalenceRules.commute_selections, # Rule 2
    # # EquivalenceRules.collapse_projections, # Rule 3
    EquivalenceRules.combineJoinCondition, # Rule 4
    EquivalenceRules.switchChildrenJoin, # Rule 5
    EquivalenceRules.associativeJoins, # Rule 6
    # EquivalenceRules.distributeSelection, #Rule 7
    EquivalenceRules.push_projections_into_join, # Rule 8
    # EquivalenceRules.joinAlgorithmVariation, # Additional Rule
]) -> List['QueryPlan']:

    initial_plan = query.clone()
    plans = { initial_plan }

    queue = deque([initial_plan])

    while queue:
        current_plan = queue.popleft()

        # Traverse the query plan tree
        nodes_to_process = deque([current_plan.root])

        while nodes_to_process:
            current_node = nodes_to_process.popleft()

            for rule in rules:
                
                transformed_nodes = rule(current_node)
                

                # Check if a transformation occurred
                if len(transformed_nodes) > 1 or (len(transformed_nodes) == 1 and transformed_nodes[0].id != current_node.id):
                    for transformed_node in transformed_nodes:
                        # print(f" - Generated transformed node with id: {transformed_node.id}")
                        # Clone the current plan to create a new plan
                        new_plan = current_plan.clone()

                        if not replace_node(new_plan.root, current_node.id, transformed_node):
                            # If we couldn't replace it in the subtree, 
                            # maybe this node *is* actually the root node
                            if current_node.id == new_plan.root.id:
                                new_plan.root = transformed_node
                                # print(f" - Replaced root node with id: {transformed_node.id}")
                            else:
                                continue
                        else:
                            # print(f" - Replaced node id: {current_node.id} with id: {transform    ed_node.id}")
                            pass

                        # Avoid adding duplicate plans using the set
                        if new_plan not in plans:
                            plans.add(new_plan)
                            queue.append(new_plan)

                    transformed_this_node = True


            # Add children to the nodes_to_process queue
            if isinstance(current_node, ProjectNode) and current_node.child:
                nodes_to_process.append(current_node.child)
            elif isinstance(current_node, UnionSelectionNode):
                for child in current_node.children:
                    nodes_to_process.append(child)
            elif isinstance(current_node, SelectionNode) and current_node.child:
                nodes_to_process.append(current_node.child)
            elif isinstance(current_node, (JoinNode, ConditionalJoinNode, NaturalJoinNode)) and isinstance(current_node.children, Pair):
                nodes_to_process.append(current_node.children.first)
                nodes_to_process.append(current_node.children.second)
            elif isinstance(current_node, SortingNode) and current_node.child:
                nodes_to_process.append(current_node.child)
            elif isinstance(current_node, LimitNode) and current_node.child:
                nodes_to_process.append(current_node.child)

    # Convert the set back to a list
    unique_plans = list(plans)
    return unique_plans


def replace_node(current: QueryNode, target_id: str, replacement: QueryNode) -> bool:
    if isinstance(current, ProjectNode):
        if current.child:
            if current.child.id == target_id:
                current.set_child(replacement)
                return True
            else:
                return replace_node(current.child, target_id, replacement)
        return False

    elif isinstance(current, UnionSelectionNode):
        for i, child in enumerate(current.children):
            if child.id == target_id:
                current.children[i] = replacement
                return True
            else:
                if replace_node(child, target_id, replacement):
                    return True
        return False

    elif isinstance(current, SelectionNode):
        if current.child:
            if current.child.id == target_id:
                current.set_child(replacement)
                return True
            else:
                return replace_node(current.child, target_id, replacement)
        return False

    elif isinstance(current, (JoinNode, ConditionalJoinNode, NaturalJoinNode)):
        if current.children:
            # Check first child
            if current.children.first.id == target_id:
                current.children.first = replacement
                return True
            # Check second child
            elif current.children.second.id == target_id:
                current.children.second = replacement
                return True
            else:
                # Recursively try to replace down the tree
                if replace_node(current.children.first, target_id, replacement):
                    return True
                if replace_node(current.children.second, target_id, replacement):
                    return True
        return False

    elif isinstance(current, SortingNode):
        if current.child:
            if current.child.id == target_id:
                current.set_child(replacement)
                return True
            else:
                return replace_node(current.child, target_id, replacement)
        return False

    elif isinstance(current, LimitNode):
        if current.child:
            if current.child.id == target_id:
                current.set_child(replacement)
                return True
            else:
                return replace_node(current.child, target_id, replacement)
        return False

    elif isinstance(current, TableNode):
        return False

    return False