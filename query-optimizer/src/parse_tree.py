from typing import Any, List, Optional
from src.lexer import Token

class Node:
    token_type: Token
    value: Any | None # value bernilai None kalau tidak ada maknanya, contohnya untuk token_type SELECT, UPDATE, dll.
               # value bernilai sesuatu jika ada maknanya, contohnya untuk token_type TABLE, ATTRIBUTE.

    def __init__(self, token_type, value=None):
        self.token_type = token_type
        self.value = value
    
    def __str__(self):
        if self.value is not None:
            return f"{self.token_type}({self.value})"
        return f"{self.token_type}"

    
    
class ParseTree:
    root: Node | None
    childs: List['ParseTree'] = []

    def __init__(self, root=None):
        if root is not None:
            self.root = root
        childs = []

    def add_child(self, child):
        if child is None:
            return
        elif isinstance(child, Node):
            self.childs.append(ParseTree(child))
        else:
            assert isinstance(child, ParseTree)
                
            self.childs.append(child)



