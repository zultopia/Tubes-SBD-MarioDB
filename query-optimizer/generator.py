from equivalence_rules import EquivalenceRules
from query_plan.query_plan import QueryPlan, QueryNode, ProjectNode, UnionSelectionNode, SelectionNode, JoinNode, ConditionalJoinNode, NaturalJoinNode, SortingNode, TableNode, Pair
from typing import List
from collections import deque


# generator.py
from collections import deque
from typing import List
from query_plan.query_plan import QueryPlan
from equivalence_rules import EquivalenceRules
from query_plan.base import QueryNode

def generate_possible_plans(query: 'QueryPlan') -> List['QueryPlan']:
    rules = [EquivalenceRules.deconstruct_conjunction,
             EquivalenceRules.commute_selections
             ]

    initial_plan = query.clone()
    plans = {initial_plan}  # Use a set to store unique plans based on __hash__ and __eq__

    queue = deque([initial_plan])  # Initialize the queue with the cloned plan

    while queue:
        current_plan = queue.popleft()
        print(f"\nProcessing plan with root id: {current_plan.root.id}")

        # Traverse the query plan tree
        nodes_to_process = deque()
        nodes_to_process.append(current_plan.root)

        while nodes_to_process:
            current_node = nodes_to_process.popleft()
            print(f"Applying rules to node: {current_node} with id: {current_node.id}")

            # Apply each rule
            for rule in rules:
                transformed_nodes = rule(current_node)

                # Check if transformation occurred
                if len(transformed_nodes) > 1 or (len(transformed_nodes) == 1 and transformed_nodes[0].id != current_node.id):
                    print(f"Transformation applied on node id: {current_node.id}")
                    for transformed_node in transformed_nodes:
                        print(f" - Generated transformed node with id: {transformed_node.id}")
                        # Clone the current plan to create a new plan
                        new_plan = current_plan.clone()

                        if current_node.id == new_plan.root.id:
                            # If the root is being replaced
                            new_plan.root = transformed_node
                            print(f" - Replaced root node with id: {transformed_node.id}")
                        else:
                            # Replace the node with matching ID
                            replaced = replace_node(new_plan.root, current_node.id, transformed_node)
                            if replaced:
                                print(f" - Replaced node id: {current_node.id} with id: {transformed_node.id}")
                            else:
                                print(f" - Failed to replace node id: {current_node.id}")
                                continue  # Skip if replacement failed

                        # Avoid adding duplicate plans using the set
                        if new_plan not in plans:
                            plans.add(new_plan)
                            queue.append(new_plan)

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
    return unique_plans



def replace_node(current: QueryNode, target_id: str, replacement: QueryNode) -> bool:
    """
    Recursively searches for the node with target_id in the query plan tree and replaces it with the replacement node.
    Returns True if replacement was successful, False otherwise.
    """
    # Handle different node types and their children
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
            if current.children.first.id == target_id:
                current.children.first = replacement
                return True
            elif current.children.second.id == target_id:
                current.children.second = replacement
                return True
            else:
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

    elif isinstance(current, TableNode):
        # TableNode has no children to replace
        return False

    else:
        # Handle other node types if any
        return False