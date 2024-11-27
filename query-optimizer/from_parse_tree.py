from typing import List, Dict, Optional
from utils import Pair
from query_plan.query_plan import QueryPlan
from query_plan.base import QueryNode
from query_plan.nodes.join_nodes import JoinCondition, ConditionalJoinNode, NaturalJoinNode
from query_plan.nodes.sorting_node import SortingNode
from query_plan.nodes.project_node import ProjectNode
from query_plan.nodes.table_node import TableNode
from query_plan.enums import JoinAlgorithm
from parse_tree import ParseTree, Node
from lexer import Token 




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
            
        # Process WHERE clause if it exists (after FROM)
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

    current_node = process_table_term(table_result_tree.childs[0])
    
    if len(table_result_tree.childs) > 1:
        tail = table_result_tree.childs[1]
        while tail and tail.childs:
            if isinstance(tail.root, str) and tail.root == "TableResultTail":
                if tail.childs[0].root.token_type == Token.NATURAL:
                    join_node = NaturalJoinNode(JoinAlgorithm.NESTED_LOOP) 
                    right_node = process_table_term(tail.childs[2])
                    join_node.set_children(Pair(current_node, right_node))
                    current_node = join_node
                elif tail.childs[0].root.token_type == Token.JOIN:
                    join_node = process_conditional_join(tail)
                    right_node = process_table_term(tail.childs[1])
                    join_node.set_children(Pair(current_node, right_node))
                    current_node = join_node
                
                tail = tail.childs[-1] 
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
def process_conditional_join(join_tree: 'ParseTree') -> ConditionalJoinNode:
    """Process conditional join and create ConditionalJoinNode."""
    conditions = []
    condition_tree = join_tree.childs[3]  # ON clause conditions
    
    def extract_condition(cond_tree: 'ParseTree') -> Optional[JoinCondition]:
        if cond_tree.root == "ConditionTerm":
            # Extract left field, operator, and right field
            left_field = extract_field_name(cond_tree.childs[0])
            op = extract_operator(cond_tree.childs[1])
            right_field = extract_field_name(cond_tree.childs[2])
            return JoinCondition(left_field, right_field, op)
        return None
    
    def extract_field_name(field_tree: 'ParseTree') -> str:
        if isinstance(field_tree, 'ParseTree') and field_tree.root == "Field":
            if len(field_tree.childs) == 1:
                return field_tree.childs[0].root.value
            else:
                return field_tree.childs[2].root.value
        return str(field_tree.root.value)
    
    def extract_operator(op_tree: 'ParseTree') -> str:
        return op_tree.childs[0].root.value
    
    condition = extract_condition(condition_tree)
    if condition:
        conditions.append(condition)
    
    return ConditionalJoinNode(JoinAlgorithm.NESTED_LOOP, conditions) 

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