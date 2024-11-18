import optimize

from typing import List
from lexer import Token

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
        # ini nyoba isi root dan childsnya, tapi queryplan belum.
        pass
    
    def get_cost(self) -> float:
        return optimize.calculate_cost(self)
    
    def optimize(self):
        # ini nyoba susun ulang tree biar lebih optimal nya dan mengisi queryplannya.
        optimize.optimize(self)
        
    
    
    
class ParsedQuery:
    parsed_query_tree: ParsedQueryTree
    query: str # ini hasil query yang sudah dimodify sehingga sesuai dengan query tree yang sudah teroptimisasi
    
    def __init__(self, unoptimized_query: str):
        self.parsed_query_tree = ParsedQueryTree(unoptimized_query)
    
    def optimize(self):
        self.optimize()
    
