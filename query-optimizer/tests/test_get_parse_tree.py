from typing import Any, List, Tuple
import pytest
from parser import parse
from query_optimizer import get_parse_tree
from parse_tree import ParseTree, Node
from lexer import Token

def compare_parse_tree(tree1: ParseTree, tree2: ParseTree) -> bool:
    if tree1.root != tree2.root:
        return False
    if len(tree1.childs) != len(tree2.childs):
        return False
    for child1, child2 in zip(tree1.childs, tree2.childs):
        if isinstance(child1, ParseTree) and isinstance(child2, ParseTree):
            if not compare_parse_tree(child1, child2):
                return False
        elif isinstance(child1, Node) and isinstance(child2, Node):
            if child1.token_type != child2.token_type or child1.value != child2.value:
                return False
        else:
            return False
    return True

def test_get_parse_tree_SELECT():
    query = "SELECT name, age FROM people;"
    expected_tree = ParseTree("Query")

    select_node = Node(Token.SELECT, "SELECT")
    select_list = ParseTree("SelectList")
    
    field1 = ParseTree("Field")
    field1.add_child(Node(Token.ATTRIBUTE, "name"))
    select_list.add_child(field1)

    select_list_ = ParseTree("SelectList_")
    select_list_.add_child(Node(Token.COMMA, ","))
    field2 = ParseTree("Field")
    field2.add_child(Node(Token.ATTRIBUTE, "age"))
    select_list_.add_child(field2)

    select_list.add_child(select_list_)
    
    from_node = Node(Token.FROM, "FROM")
    from_list = ParseTree("FromList")
    table_node = ParseTree("TableReference")
    table_node.add_child(Node(Token.TABLE, "people"))
    from_list.add_child(table_node)

    expected_tree.add_child(select_node)
    expected_tree.add_child(select_list)
    expected_tree.add_child(from_node)
    expected_tree.add_child(from_list)
    expected_tree.add_child(Node(Token.SEMICOLON, ";"))


    actual_tree = get_parse_tree(query)
    assert compare_parse_tree(actual_tree, expected_tree), f"Expected: {expected_tree}, Got: {actual_tree}"

#add more tests later
    

