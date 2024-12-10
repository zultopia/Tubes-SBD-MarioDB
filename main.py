# semua komponen jadiin satu di sini
from QueryProcessor.classes import QueryProcessor
from QueryOptimizer.parse_tree import ParseTree

QueryProcessor = QueryProcessor()
while(True) :
    query_string = input('Please enter your query: ')
    if (query_string == "\\q") :
        break
    QueryProcessor.execute_query(query_string)