from equivalence_rules import EquivalenceRules
from query_plan.query_plan import (
    QueryPlan, QueryNode, ProjectNode, UnionSelectionNode, SelectionNode,
    JoinNode, ConditionalJoinNode, NaturalJoinNode, SortingNode, TableNode, Pair
)
from typing import List
from collections import deque

def generate_possible_plans(query: 'QueryPlan') -> List['QueryPlan']:
    # Include only the deconstruct_conjunction rule
    rules = [EquivalenceRules.deconstruct_conjunction]

    initial_plan = query.clone()
    plans = {initial_plan}  # Use a set to store unique plans based on __hash__ and __eq__

    queue = deque([initial_plan])  # Initialize the queue with the cloned plan

    while queue:
        current_plan = queue.popleft()
        print(f"\nProcessing plan with root id: {current_plan.root.id}")

        # Traverse the query plan tree
        nodes_to_process = deque([current_plan.root])

        while nodes_to_process:
            current_node = nodes_to_process.popleft()
            print(f"Applying rules to node: {current_node} with id: {current_node.id}")

            transformed_this_node = False

            for rule in rules:
                transformed_nodes = rule(current_node)

                # Check if a transformation occurred
                if len(transformed_nodes) > 1 or (len(transformed_nodes) == 1 and transformed_nodes[0].id != current_node.id):
                    print(f"Transformation applied on node id: {current_node.id}")
                    for transformed_node in transformed_nodes:
                        print(f" - Generated transformed node with id: {transformed_node.id}")
                        # Clone the current plan to create a new plan
                        new_plan = current_plan.clone()

                        if not replace_node(new_plan.root, current_node.id, transformed_node):
                            # If we couldn't replace it in the subtree, 
                            # maybe this node *is* actually the root node
                            if current_node.id == new_plan.root.id:
                                new_plan.root = transformed_node
                                print(f" - Replaced root node with id: {transformed_node.id}")
                            else:
                                print(f" - Failed to replace node id: {current_node.id}")
                                continue
                        else:
                            print(f" - Replaced node id: {current_node.id} with id: {transformed_node.id}")

                        # Avoid adding duplicate plans using the set
                        if new_plan not in plans:
                            plans.add(new_plan)
                            queue.append(new_plan)

                    transformed_this_node = True
                    break

            if transformed_this_node:
                # Node changed, don't process its children now
                continue

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

    # Convert the set back to a list
    unique_plans = list(plans)
    print(f"\nGenerated plans:{len(unique_plans)}")
    for plan in unique_plans:
        print("\nQuery Plan:")
        print(plan)
    return unique_plans


def replace_node(current: QueryNode, target_id: str, replacement: QueryNode) -> bool:
    if isinstance(current, ProjectNode):
        if current.child:
            if current.child.id == target_id:
                old_child_id = current.child.id
                current.set_child(replacement)
                print(f"Replaced ProjectNode child {old_child_id} with {replacement.id}")
                return True
            else:
                return replace_node(current.child, target_id, replacement)
        return False

    elif isinstance(current, UnionSelectionNode):
        for i, child in enumerate(current.children):
            if child.id == target_id:
                old_child_id = child.id
                current.children[i] = replacement
                print(f"Replaced UnionSelectionNode child {old_child_id} with {replacement.id}")
                return True
            else:
                if replace_node(child, target_id, replacement):
                    return True
        return False

    elif isinstance(current, SelectionNode):
        if current.child:
            if current.child.id == target_id:
                old_child_id = current.child.id
                current.set_child(replacement)
                print(f"Replaced SelectionNode child {old_child_id} with {replacement.id}")
                return True
            else:
                return replace_node(current.child, target_id, replacement)
        return False

    elif isinstance(current, (JoinNode, ConditionalJoinNode, NaturalJoinNode)):
        if current.children:
            # Check first child
            if current.children.first.id == target_id:
                old_child_id = current.children.first.id
                current.children.first = replacement
                print(f"Replaced JoinNode first child {old_child_id} with {replacement.id}")
                return True
            # Check second child
            elif current.children.second.id == target_id:
                old_child_id = current.children.second.id
                current.children.second = replacement
                print(f"Replaced JoinNode second child {old_child_id} with {replacement.id}")
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
                old_child_id = current.child.id
                current.set_child(replacement)
                print(f"Replaced SortingNode child {old_child_id} with {replacement.id}")
                return True
            else:
                return replace_node(current.child, target_id, replacement)
        return False

    elif isinstance(current, TableNode):
        return False

    return False