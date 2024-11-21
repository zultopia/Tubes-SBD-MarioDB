import sys
sys.setrecursionlimit(1000) # tambahin jika perlu
from parse_tree import ParseTree
from query_optimizer import get_parse_tree, get_optimized_query_plan

query_string = input('Please enter your query: ')

tree = get_parse_tree(query_string)

print(tree)