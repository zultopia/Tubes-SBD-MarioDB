from src.parser import parse
from src.parse_tree import ParseTree
from src.query_plan_tree import from_parse_tree
from src.optimizer import optimize

def get_parse_tree(query_string) -> ParseTree:
    return parse(query_string)

def get_optimized_query_plan(query_string):
    query_plan_tree = from_parse_tree(get_parse_tree(query_string))
    optimize(query_plan_tree)
    return query_plan_tree

