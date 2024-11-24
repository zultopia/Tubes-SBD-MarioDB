from typing import Any, List, Tuple
import pytest
from parser import parse
from query_optimizer import get_parse_tree
from parse_tree import ParseTree, Node
from lexer import Token

    
def compare_parse_tree(tree1: ParseTree, tree2: ParseTree) -> bool:
    if str(tree1.root) != str(tree2.root):
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


def test_get_parse_tree_select_multiple_attributes():
    query = "SELECT age1, prodi FROM user;"
    actual_tree = get_parse_tree(query)
    
    expected_tree = ParseTree()
    expected_tree.root = "Query"

    select_node = Node(Token.SELECT, "SELECT")
    expected_tree.add_child(ParseTree(select_node))

    select_list_tree = ParseTree()
    select_list_tree.root = "SelectList"
    
    field1_tree = ParseTree()
    field1_tree.root = "Field"
    field1_tree.add_child(Node(Token.ATTRIBUTE, "age1"))
    select_list_tree.add_child(field1_tree)

    select_list_tail_tree = ParseTree()
    select_list_tail_tree.root = "SelectList_"

    select_list_tail_tree.add_child(Node(Token.COMMA, ","))

    field2_tree = ParseTree()
    field2_tree.root = "Field"
    field2_tree.add_child(Node(Token.ATTRIBUTE, "prodi"))
    select_list_tail_tree.add_child(field2_tree)

    select_list_tree.add_child(select_list_tail_tree)
    expected_tree.add_child(select_list_tree)

    from_node = Node(Token.FROM, "FROM")
    expected_tree.add_child(ParseTree(from_node))

    from_list_tree = ParseTree()
    from_list_tree.root = "FromList"

    table_ref_tree = ParseTree()
    table_ref_tree.root = "TableReference"
    table_ref_tree.add_child(Node(Token.TABLE, "user"))
    from_list_tree.add_child(table_ref_tree)

    expected_tree.add_child(from_list_tree)

    semicolon_node = Node(Token.SEMICOLON, ";")
    expected_tree.add_child(ParseTree(semicolon_node))

    assert compare_parse_tree(actual_tree, expected_tree)

    
def test_get_parse_tree_select_with_join():
    query = "SELECT age1, prodi FROM user JOIN prodis ON user.prodiid = prodis.prodiid;"
    actual_tree = get_parse_tree(query)

    expected_tree = ParseTree()
    expected_tree.root = "Query"

    select_node = Node(Token.SELECT, "SELECT")
    expected_tree.add_child(ParseTree(select_node))

    select_list_tree = ParseTree()
    select_list_tree.root = "SelectList"

    field1_tree = ParseTree()
    field1_tree.root = "Field"
    field1_tree.add_child(Node(Token.ATTRIBUTE, "age1"))
    select_list_tree.add_child(field1_tree)

    select_list_tail_tree = ParseTree()
    select_list_tail_tree.root = "SelectList_"

    select_list_tail_tree.add_child(Node(Token.COMMA, ","))

    field2_tree = ParseTree()
    field2_tree.root = "Field"
    field2_tree.add_child(Node(Token.ATTRIBUTE, "prodi"))
    select_list_tail_tree.add_child(field2_tree)

    select_list_tree.add_child(select_list_tail_tree)
    expected_tree.add_child(select_list_tree)

    from_node = Node(Token.FROM, "FROM")
    expected_tree.add_child(ParseTree(from_node))

    from_list_tree = ParseTree()
    from_list_tree.root = "FromList"

    table_ref_tree = ParseTree()
    table_ref_tree.root = "TableReference"
    table_ref_tree.add_child(Node(Token.TABLE, "user"))
    from_list_tree.add_child(table_ref_tree)

    from_list_tail_tree = ParseTree()
    from_list_tail_tree.root = "FromList_"

    join_table_tree = ParseTree()
    join_table_tree.root = "TableReference"
    join_table_tree.add_child(Node(Token.TABLE, "prodis"))
    from_list_tail_tree.add_child(join_table_tree)

    join_condition_tree = ParseTree()
    join_condition_tree.root = "Field"
    join_condition_tree.add_child(Node(Token.TABLE, "user"))
    join_condition_tree.add_child(Node(Token.DOT, "."))
    join_condition_tree.add_child(Node(Token.ATTRIBUTE, "prodiid"))
    join_condition_tree.add_child(Node(Token.EQ, "="))
    join_condition_tree.add_child(Node(Token.TABLE, "prodis"))
    join_condition_tree.add_child(Node(Token.DOT, "."))
    join_condition_tree.add_child(Node(Token.ATTRIBUTE, "prodiid"))
    from_list_tail_tree.add_child(join_condition_tree)

    from_list_tree.add_child(from_list_tail_tree)
    expected_tree.add_child(from_list_tree)

    semicolon_node = Node(Token.SEMICOLON, ";")
    expected_tree.add_child(ParseTree(semicolon_node))

    assert compare_parse_tree(actual_tree, expected_tree)
    

def test_where_clause_with_float():
    query = "SELECT age1 FROM user WHERE age1 > 20.234;"
    actual_tree = get_parse_tree(query)

    expected_tree = ParseTree()
    expected_tree.root = "Query"

    select_node = Node(Token.SELECT, "SELECT")
    expected_tree.add_child(ParseTree(select_node))

    select_list_tree = ParseTree()
    select_list_tree.root = "SelectList"

    field_tree = ParseTree()
    field_tree.root = "Field"
    field_tree.add_child(Node(Token.ATTRIBUTE, "age1"))
    select_list_tree.add_child(field_tree)

    expected_tree.add_child(select_list_tree)

    from_node = Node(Token.FROM, "FROM")
    expected_tree.add_child(ParseTree(from_node))

    from_list_tree = ParseTree()
    from_list_tree.root = "FromList"

    table_ref_tree = ParseTree()
    table_ref_tree.root = "TableReference"
    table_ref_tree.add_child(Node(Token.TABLE, "user"))
    from_list_tree.add_child(table_ref_tree)

    expected_tree.add_child(from_list_tree)

    where_node = Node(Token.WHERE, "WHERE")
    expected_tree.add_child(ParseTree(where_node))

    condition_tree = ParseTree()
    condition_tree.root = "Field"

    field_condition = ParseTree()
    field_condition.root = "Field"
    field_condition.add_child(Node(Token.ATTRIBUTE, "age1"))

    condition_tree.add_child(field_condition)
    condition_tree.add_child(Node(Token.GREATER, ">"))
    condition_tree.add_child(Node(Token.NUMBER, 20.234))

    expected_tree.add_child(condition_tree)

    semicolon_node = Node(Token.SEMICOLON, ";")
    expected_tree.add_child(ParseTree(semicolon_node))

    assert compare_parse_tree(actual_tree, expected_tree)
    
def test_get_parse_tree_complex_query():
    query = "SELECT user.name, user.age FROM users JOIN addresses ON users.id AS uid = addresses.user_id aid WHERE user.age > 30;"
    actual_tree = get_parse_tree(query)

    expected_tree = ParseTree()
    expected_tree.root = "Query"

    select_node = Node(Token.SELECT, "SELECT")
    expected_tree.add_child(ParseTree(select_node))

    select_list_tree = ParseTree()
    select_list_tree.root = "SelectList"

    field1_tree = ParseTree()
    field1_tree.root = "Field"
    field1_tree.add_child(Node(Token.TABLE, "user"))
    field1_tree.add_child(Node(Token.DOT, "."))
    field1_tree.add_child(Node(Token.ATTRIBUTE, "name"))
    select_list_tree.add_child(field1_tree)

    select_list_tail_tree = ParseTree()
    select_list_tail_tree.root = "SelectList_"
    select_list_tail_tree.add_child(Node(Token.COMMA, ","))

    field2_tree = ParseTree()
    field2_tree.root = "Field"
    field2_tree.add_child(Node(Token.TABLE, "user"))
    field2_tree.add_child(Node(Token.DOT, "."))
    field2_tree.add_child(Node(Token.ATTRIBUTE, "age"))
    select_list_tail_tree.add_child(field2_tree)

    select_list_tree.add_child(select_list_tail_tree)
    expected_tree.add_child(select_list_tree)

    from_node = Node(Token.FROM, "FROM")
    expected_tree.add_child(ParseTree(from_node))

    from_list_tree = ParseTree()
    from_list_tree.root = "FromList"

    table_ref_tree = ParseTree()
    table_ref_tree.root = "TableReference"
    table_ref_tree.add_child(Node(Token.TABLE, "users"))
    from_list_tree.add_child(table_ref_tree)

    from_list_tail_tree = ParseTree()
    from_list_tail_tree.root = "FromList_"

    join_table_tree = ParseTree()
    join_table_tree.root = "TableReference"
    join_table_tree.add_child(Node(Token.TABLE, "addresses"))
    from_list_tail_tree.add_child(join_table_tree)

    join_condition_tree = ParseTree()
    join_condition_tree.root = "Field"
    join_condition_tree.add_child(Node(Token.TABLE, "users"))
    join_condition_tree.add_child(Node(Token.DOT, "."))
    join_condition_tree.add_child(Node(Token.ATTRIBUTE, "id"))
    join_condition_tree.add_child(Node(Token.AS, "AS"))
    join_condition_tree.add_child(Node(Token.TABLE, "uid"))
    join_condition_tree.add_child(Node(Token.EQ, "="))
    join_condition_tree.add_child(Node(Token.TABLE, "addresses"))
    join_condition_tree.add_child(Node(Token.DOT, "."))
    join_condition_tree.add_child(Node(Token.ATTRIBUTE, "user_id"))
    join_condition_tree.add_child(Node(Token.TABLE, "aid"))
    from_list_tail_tree.add_child(join_condition_tree)

    from_list_tree.add_child(from_list_tail_tree)
    expected_tree.add_child(from_list_tree)

    where_node = Node(Token.WHERE, "WHERE")
    expected_tree.add_child(ParseTree(where_node))

    condition_tree = ParseTree()
    condition_tree.root = "Field"

    field_condition = ParseTree()
    field_condition.root = "Field"
    field_condition.add_child(Node(Token.TABLE, "user"))
    field_condition.add_child(Node(Token.DOT, "."))
    field_condition.add_child(Node(Token.ATTRIBUTE, "age"))

    condition_tree.add_child(field_condition)
    condition_tree.add_child(Node(Token.GREATER, ">"))
    condition_tree.add_child(Node(Token.NUMBER, 30))

    expected_tree.add_child(condition_tree)

    semicolon_node = Node(Token.SEMICOLON, ";")
    expected_tree.add_child(ParseTree(semicolon_node))

    assert compare_parse_tree(actual_tree, expected_tree)

    
