from typing import Any, List, Optional
from src.lexer import Token
extra_references = []

# class Node hanya untuk token
class Node:
    token_type: Token
    value: Any

    def __init__(self, token_type, value=None):
        global extra_references
        extra_references.append(self)
        self.token_type = token_type
        self.value = value
    
    def __str__(self):
        assert self.value is not None
        return f"[{self.token_type}]({self.value})"


    
    
class ParseTree:
    root: Node | str # root dapat bernilai string untuk auxiliary token yang muncul sebab penggunaan nonterminal pada grammar 
    childs = []

    def __init__(self, root=None):
        if root is not None:
            self.root = root
        else:
            self.root = None
        self.childs = []
        

    def add_child(self, child):
        if child is None:
            return
        elif isinstance(child, Node):
            self.childs.append(ParseTree(child))
        else:                
            self.childs.append(child)


    def __str__(self, level=0):
        result = "  " * level + str(self.root) + "\n"
        for child in self.childs:
            result += child.__str__(level + 1)
        return result