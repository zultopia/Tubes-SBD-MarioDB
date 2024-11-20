from typing import List
from src.grammar import SQLGrammar
from src.lexer import Token

class Node:
    def __init__(self):
        pass
        
        
    token_type: Token
    value: str | None # value bernilai None kalau tidak ada maknanya, contohnya untuk token_type SELECT, UPDATE, dll.
               # value bernilai sesuatu jika ada maknanya, contohnya untuk token_type TABLE, ATTRIBUTE.
    
    
class QueryPlan:
    # Ini terserah mau dibuat kayak gimana.
    # Yang jelas minimal harus ada:
    #   1. select: mengconsider index-index yang ada, operasi-operasi yang dilakukan
    #   2. sorting: external sort-merge
    #   3. join: (indexed) nested-loop, block nested-loop, merge, hybrid merge, hash.
    # Apakah ada yang kurang?
    pass

class ParsedQueryTree:
    root: Node
    childs: List[Node] = []
    
    query_plan: QueryPlan | None
    
    def __init__(self, unoptimized_query):
        self.parse(unoptimized_query)
        
    def parse(self, unoptimized_query: str):
        from tests.test_lexer import mock_tokenize as tokenize
        tokens = tokenize(unoptimized_query)
        print(tokens)

        SQL_grammar = SQLGrammar(tokens)
        SQL_grammar.Query()



    
    def get_cost(self) -> float:
        # Nyoman
        pass
    
    def optimize(self):
        # Nyoman
        pass
        
    
    
    
class ParsedQuery:
    parsed_query_tree: ParsedQueryTree
    query: str # ini hasil query yang sudah dimodify sehingga sesuai dengan query tree yang sudah teroptimisasi
    
    def __init__(self, unoptimized_query: str):
        self.parsed_query_tree = ParsedQueryTree(unoptimized_query)
    
    def optimize(self):
        self.parsed_query_tree.optimize()
    
