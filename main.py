# semua komponen jadiin satu di sini
from QueryProcessor.classes import QueryProcessor
from QueryOptimizer.parse_tree import ParseTree

QueryProcessor = QueryProcessor()
query_string = input('Please enter your query: ')
QueryProcessor.execute_query(query_string)