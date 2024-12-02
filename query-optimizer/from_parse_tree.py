from typing import List, Dict, Optional
from utils import Pair
from query_plan.query_plan import QueryPlan
from query_plan.base import QueryNode
from query_plan.nodes.join_nodes import JoinCondition, ConditionalJoinNode, NaturalJoinNode
from query_plan.nodes.sorting_node import SortingNode
from query_plan.nodes.project_node import ProjectNode
from query_plan.nodes.table_node import TableNode
from query_plan.nodes.selection_node import SelectionNode, SelectionCondition, UnionSelectionNode
from query_plan.enums import JoinAlgorithm, SelectionOperation
from parse_tree import ParseTree, Node
from lexer import Token 


def from_parse_tree(parse_tree: ParseTree) -> QueryPlan:
    if isinstance(parse_tree.root, str) and parse_tree.root == "Query":
        project_node = process_select_list(parse_tree.childs[1])
        
        # First process the base table structure (FROM/JOIN)
        table_node = None
        for i, child in enumerate(parse_tree.childs):
            if isinstance(child.root, Node) and child.root.token_type == Token.FROM:
                table_node = process_from_list(parse_tree.childs[i + 1])
                break

        # Process WHERE with possible unions
        where_index = None
        for i, child in enumerate(parse_tree.childs):
            if isinstance(child.root, Node) and child.root.token_type == Token.WHERE:
                where_index = i
                break

        if where_index is not None:
            condition_node = process_where_clause(parse_tree.childs[where_index + 1])
            if isinstance(condition_node, UnionSelectionNode):
                # For each branch in the union, create a complete copy of the table structure
                for selection_node in condition_node.children:
                    table_copy = process_from_list(parse_tree.childs[3])  # Create fresh copy of table structure
                    selection_node.set_child(table_copy)
            else:
                condition_node.set_child(table_node)
            table_node = condition_node

        project_node.set_child(table_node)
        
        return QueryPlan(project_node)

def process_select_list(select_list_tree: ParseTree) -> ProjectNode:
    """Process SELECT list and create ProjectNode."""
    attributes: List[str] = []
    
    def extract_field(field_tree: ParseTree) -> str:
        if isinstance(field_tree.root, Node):
            return field_tree.root.value
            
        if len(field_tree.childs) == 1:  
            return field_tree.childs[0].root.value
        else:  
            return f"{field_tree.childs[0].root.value}.{field_tree.childs[2].root.value}"
    
    if select_list_tree.root == "SelectList":
        attributes.append(extract_field(select_list_tree.childs[0]))
        
        if len(select_list_tree.childs) > 1 and select_list_tree.childs[1]:
            current = select_list_tree.childs[1]
            while current and isinstance(current, ParseTree) and current.root == "SelectListTail":
                if len(current.childs) >= 2:
                    attributes.append(extract_field(current.childs[1]))
                if len(current.childs) >= 3 and current.childs[2]:
                    current = current.childs[2]
                else:
                    break
    else:
        attributes.append(extract_field(select_list_tree))

    return ProjectNode(attributes)

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
                if len(tail.childs) >= 1:  
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
    conditions: List[JoinCondition] = []
    condition_tree = join_tree.childs[3]  
    
    def process_condition_term(cond_term: ParseTree) -> Optional[JoinCondition]:
        if len(cond_term.childs) >= 3:
            left = extract_field_value(cond_term.childs[0])
            operator = cond_term.childs[1].childs[0].root.value
            right = extract_field_value(cond_term.childs[2])
            return JoinCondition(left, right, operator)  
        return None

    def extract_field_value(field_tree: ParseTree) -> str:
        if field_tree.root == "Field":
            if len(field_tree.childs) == 3:  
                return f"{field_tree.childs[0].root.value}.{field_tree.childs[2].root.value}"
            else:  
                return field_tree.childs[0].root.value
        return field_tree.root.value

    if condition_tree.root == "Condition":
        current = condition_tree.childs[0]
        if current.root == "AndCondition":
            cond = process_condition_term(current.childs[0])
            if cond:
                conditions.append(cond)
            
            if len(current.childs) > 1 and current.childs[1]:
                tail = current.childs[1]
                while tail and tail.root == "AndConditionTail":
                    if len(tail.childs) >= 2:
                        cond = process_condition_term(tail.childs[1])
                        if cond:
                            conditions.append(cond)
                    if len(tail.childs) > 2:
                        tail = tail.childs[2]
                    else:
                        break

    return ConditionalJoinNode(JoinAlgorithm.HASH, conditions)

def process_where_clause(condition_tree: ParseTree) -> QueryNode:
    """Process WHERE clause and create either SelectionNode or UnionSelectionNode."""
    def convert_operator(op_token: Token) -> SelectionOperation:
        """Convert Token to SelectionOperation."""
        token_to_op = {
            Token.GREATER: SelectionOperation.GREATER,
            Token.GREATER_EQ: SelectionOperation.GREATER_EQ,
            Token.LESS: SelectionOperation.LESS,
            Token.LESS_EQ: SelectionOperation.LESS_EQ,
            Token.EQ: SelectionOperation.EQ,
            Token.NEQ: SelectionOperation.NEQ
        }
        return token_to_op.get(op_token, SelectionOperation.EQ)

    def extract_condition_term(cond_term: ParseTree) -> Optional[SelectionCondition]:
        if len(cond_term.childs) == 3:
            left = extract_operand(cond_term.childs[0])
            op_token = cond_term.childs[1].childs[0].root.token_type
            op = convert_operator(op_token)
            right = extract_operand(cond_term.childs[2])
            return SelectionCondition(left, right, op)
        return None
    
    def extract_operand(tree: ParseTree) -> str:
        if isinstance(tree, ParseTree):
            if tree.root == "Field":
                return extract_field_name(tree)
            elif isinstance(tree.root, Node):
                if tree.root.token_type in [Token.NUMBER, Token.STRING]:
                    return str(tree.root.value)
        return str(tree)
    
    def extract_field_name(field_tree: ParseTree) -> str:
        if len(field_tree.childs) == 1:
            return field_tree.childs[0].root.value
        else:
            return f"{field_tree.childs[0].root.value}.{field_tree.childs[2].root.value}"

    def process_or_conditions(condition_tree: ParseTree) -> List[SelectionNode]:
        selection_nodes = []
        conditions = []

        def process_and_condition(and_condition: ParseTree):
            conditions.clear()
            # Process first condition
            cond = extract_condition_term(and_condition.childs[0])
            if cond:
                conditions.append(cond)
            
            # Process AndConditionTail if exists
            if len(and_condition.childs) > 1 and and_condition.childs[1]:
                tail = and_condition.childs[1]
                while tail and tail.root == "AndConditionTail":
                    if len(tail.childs) >= 2:
                        cond = extract_condition_term(tail.childs[1])
                        if cond:
                            conditions.append(cond)
                    if len(tail.childs) > 2:
                        tail = tail.childs[2]
                    else:
                        break
            
            if conditions:
                selection_nodes.append(SelectionNode(conditions.copy()))
        
        if condition_tree.root == "Condition":
            current = condition_tree.childs[0]  # First AndCondition
            process_and_condition(current)
            
            # Process ConditionTail (OR conditions)
            if len(condition_tree.childs) > 1 and condition_tree.childs[1]:
                tail = condition_tree.childs[1]
                while tail and tail.root == "ConditionTail":
                    if len(tail.childs) >= 2:  # Should have "OR" and AndCondition
                        process_and_condition(tail.childs[1])
                    if len(tail.childs) > 2:
                        tail = tail.childs[2]  # Next ConditionTail
                    else:
                        break
        
        return selection_nodes

    selection_nodes = process_or_conditions(condition_tree)
    
    # If there's only one selection node (no OR conditions), return it directly
    if len(selection_nodes) == 1:
        return selection_nodes[0]
    # If there are multiple selections (OR conditions), wrap them in a UnionSelectionNode
    elif len(selection_nodes) > 1:
        return UnionSelectionNode(selection_nodes)
    else:
        raise ValueError("No conditions found in WHERE clause")

def process_order_by(order_tree: ParseTree) -> SortingNode:
    """Process ORDER BY clause and create SortingNode."""
    attributes: List[str] = []
    
    def extract_field(field_tree: ParseTree) -> str:
        if len(field_tree.childs) == 1:
            return field_tree.childs[0].root.value
        else:
            return f"{field_tree.childs[0].root.value}.{field_tree.childs[2].root.value}"
    
    if order_tree.root == "Field":
        attributes.append(extract_field(order_tree))
    
    return SortingNode(attributes)  