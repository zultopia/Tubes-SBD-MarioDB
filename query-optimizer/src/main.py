from src.parser import ParsedQuery

query_string = input("Please enter the query: ")
parsed_query = ParsedQuery(query_string)

parsed_query.parsed_query_tree.optimize()