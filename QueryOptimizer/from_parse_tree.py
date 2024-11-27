from typing import List, Dict, Optional
from QueryOptimizer.utils import Pair
from QueryOptimizer.query_plan.query_plan import QueryPlan
from QueryOptimizer.query_plan.base import QueryNode
from QueryOptimizer.query_plan.nodes.join_nodes import JoinCondition, ConditionalJoinNode, NaturalJoinNode
from QueryOptimizer.query_plan.nodes.sorting_node import SortingNode
from QueryOptimizer.query_plan.nodes.project_node import ProjectNode
from QueryOptimizer.query_plan.nodes.table_node import TableNode
from QueryOptimizer.query_plan.enums import JoinAlgorithm
from QueryOptimizer.parse_tree import ParseTree, Node
from QueryOptimizer.lexer import Token 


def from_parse_tree(parse_tree: ParseTree) -> QueryPlan:
    """Convert a parse tree to a query plan."""
    if isinstance(parse_tree.root, str) and parse_tree.root == "Query":
        # Process SELECT list
        project_node = process_select_list(parse_tree.childs[1])
        
        # Process FROM clause
        table_node = None
        for i, child in enumerate(parse_tree.childs):
            if isinstance(child.root, Node) and child.root.token_type == Token.FROM:
                table_node = process_from_list(parse_tree.childs[i + 1])
                break
                
        if table_node:
            project_node.set_child(table_node)
            
        current_node = table_node
        for i in range(4, len(parse_tree.childs)):
            if isinstance(parse_tree.childs[i].root, Node):
                if parse_tree.childs[i].root.token_type == Token.WHERE:
                    condition_node = process_where_clause(parse_tree.childs[i + 1])
                    if isinstance(condition_node, QueryNode) and isinstance(current_node, QueryNode):
                        condition_node.set_child(current_node)
                        current_node = condition_node
                elif parse_tree.childs[i].root.token_type == Token.ORDER_BY:
                    sort_node = process_order_by(parse_tree.childs[i + 1])
                    if isinstance(sort_node, QueryNode) and isinstance(current_node, QueryNode):
                        sort_node.set_child(current_node)
                        current_node = sort_node

        return QueryPlan(project_node)
    else:
        raise ValueError("Invalid parse tree structure")

def process_select_list(select_list_tree: ParseTree) -> ProjectNode:
    """Process SELECT list and create ProjectNode."""
    conditions: List[str] = []
    
    def extract_field(field_tree: ParseTree) -> str:
        if isinstance(field_tree.root, Node):
            return field_tree.root.value
            
        if len(field_tree.childs) == 1:  
            return field_tree.childs[0].root.value
        else:  
            return f"{field_tree.childs[0].root.value}.{field_tree.childs[2].root.value}"
    
    if select_list_tree.root == "SelectList":
        conditions.append(extract_field(select_list_tree.childs[0]))
        
        if len(select_list_tree.childs) > 1:
            current = select_list_tree.childs[1]
            while current is not None and current.childs:
                conditions.append(extract_field(current.childs[1]))
                current = current.childs[2]
    else:
        conditions.append(extract_field(select_list_tree))

    return ProjectNode(conditions)

def process_from_list(from_list_tree: ParseTree) -> QueryNode:
    """Process FROM list and create appropriate nodes."""
    if not from_list_tree.childs:
        raise ValueError("Empty FROM list")
    return process_table_result(from_list_tree.childs[0])

def process_table_result(table_result_tree: ParseTree) -> QueryNode:
    """Process table result (handles JOINs)."""
    if not table_result_tree.childs:
        raise ValueError("Empty table result")

    # Process first table term
    current_node = process_table_term(table_result_tree.childs[0])
    
    # Process joins in TableResultTail if it exists
    if len(table_result_tree.childs) > 1:
        tail = table_result_tree.childs[1]
        while tail and tail.childs:
            if isinstance(tail.root, str) and tail.root == "TableResultTail":
                if len(tail.childs) >= 1:  # Make sure we have at least one child
                    if tail.childs[0].root.token_type == Token.NATURAL:
                        # Natural Join
                        join_node = NaturalJoinNode(JoinAlgorithm.HASH)
                        right_node = process_table_term(tail.childs[2])
                        join_node.set_children(Pair(current_node, right_node))
                        current_node = join_node
                    elif tail.childs[0].root.token_type == Token.JOIN:
                        # Conditional Join
                        join_node = process_conditional_join(tail)
                        right_node = process_table_term(tail.childs[1])
                        join_node.set_children(Pair(current_node, right_node))
                        current_node = join_node
                
                if len(tail.childs) > 4:
                    tail = tail.childs[4] 
                else:
                    break
            else:
                break
            
    return current_node

def process_table_term(table_term_tree: ParseTree) -> QueryNode:
    """Process table term and create TableNode."""
    if not table_term_tree.childs:
        raise ValueError("Empty table term")

    if isinstance(table_term_tree.root, Node) and table_term_tree.root.token_type == Token.TABLE:
        return TableNode(table_term_tree.root.value)

    if table_term_tree.childs[0].root.token_type == Token.TABLE:
        return TableNode(table_term_tree.childs[0].root.value)
    elif table_term_tree.childs[0].root.token_type == Token.OPEN_PARANTHESIS:
        return process_table_result(table_term_tree.childs[1])
    else:
        raise ValueError(f"Unexpected token in table term: {table_term_tree.childs[0].root}")

def process_conditional_join(join_tree: ParseTree) -> ConditionalJoinNode:
    """Process conditional join and create ConditionalJoinNode."""
    conditions = []
    condition_tree = join_tree.childs[3]  # ON clause conditions
    
    def process_condition_term(cond_term: ParseTree) -> Optional[JoinCondition]:
        if cond_term.root == "ConditionTerm":
            left_field = extract_field_value(cond_term.childs[0])
            operator = cond_term.childs[1].childs[0].root.value
            right_field = extract_field_value(cond_term.childs[2])
            return JoinCondition(left_field, right_field, operator)
        return None

    def extract_field_value(field_tree: ParseTree) -> str:
        if field_tree.root == "Field":
            if len(field_tree.childs) == 3:  # Table.Attribute form
                return f"{field_tree.childs[0].root.value}.{field_tree.childs[2].root.value}"
            else:  # Simple attribute form
                return field_tree.childs[0].root.value
        return field_tree.root.value

    def process_and_condition(and_cond: ParseTree):
        if and_cond.root == "AndCondition":
            # Process the first condition
            cond = process_condition_term(and_cond.childs[0])
            if cond:
                conditions.append(cond)
                
            # Process AndConditionTail if it exists
            if len(and_cond.childs) > 1 and and_cond.childs[1]:
                tail = and_cond.childs[1]
                if tail.root == "AndConditionTail":
                    # Process each condition in the tail
                    cond = process_condition_term(tail.childs[1])
                    if cond:
                        conditions.append(cond)
                    # Process nested AndConditionTail if it exists
                    if len(tail.childs) > 2 and tail.childs[2]:
                        process_and_condition_tail(tail.childs[2])

    def process_and_condition_tail(tail: ParseTree):
        if tail and tail.root == "AndConditionTail" and tail.childs:
            cond = process_condition_term(tail.childs[1])
            if cond:
                conditions.append(cond)
            if len(tail.childs) > 2:
                process_and_condition_tail(tail.childs[2])

    if condition_tree.root == "Condition":
        process_and_condition(condition_tree.childs[0])
    
    return ConditionalJoinNode(JoinAlgorithm.HASH, conditions)

def process_where_clause(condition_tree: 'ParseTree') -> ProjectNode:
    """Process WHERE clause and create ProjectNode with conditions."""
    conditions = []
    
    def extract_condition(tree: 'ParseTree') -> str:
        if tree.root == "ConditionTerm":
            if len(tree.childs) == 3:  # Simple comparison
                left = extract_operand(tree.childs[0])
                op = tree.childs[1].childs[0].root.value
                right = extract_operand(tree.childs[2])
                return f"{left} {op} {right}"
        return ""
    
    def extract_operand(tree: 'ParseTree') -> str:
        if isinstance(tree, 'ParseTree'):
            if tree.root == "Field":
                return extract_field_name(tree)
            elif hasattr(tree.root, 'token_type'):
                if tree.root.token_type in [Token.NUMBER, Token.STRING]:
                    return str(tree.root.value)
        return str(tree)
    
    def extract_field_name(field_tree: 'ParseTree') -> str:
        if len(field_tree.childs) == 1:
            return field_tree.childs[0].root.value
        else:
            return f"{field_tree.childs[0].root.value}.{field_tree.childs[2].root.value}"
    
    # Extract conditions
    conditions.append(extract_condition(condition_tree))
    
    return ProjectNode(conditions)

def process_order_by(order_tree: 'ParseTree') -> SortingNode:
    """Process ORDER BY clause and create SortingNode."""
    attributes = []
    
    def extract_field(field_tree: 'ParseTree') -> str:
        if len(field_tree.childs) == 1:
            return field_tree.childs[0].root.value
        else:
            return f"{field_tree.childs[0].root.value}.{field_tree.childs[2].root.value}"
    
    # Extract sorting attributes
    if order_tree.root == "Field":
        attributes.append(extract_field(order_tree))
    
    return SortingNode(attributes)