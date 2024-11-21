from typing import Any, List, Tuple
from src.lexer import Token, Lexer
from src.optimizer import get_cost, optimize
from src.parse_tree import Node, ParseTree


class SQLGrammar:
    def __init__(self, tokens):
        self.tokens: List[Tuple[Token, Any]] = tokens
        self.pos = 0

    def current_token(self):
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def match(self, expected_token: Token):
        if self.current_token()[0] == expected_token:
            self.pos += 1
        else:
            raise SyntaxError(f"Expected {expected_token}, found {self.current_token()}")

    def Query(self) -> ParseTree:
        print("Query")
        """Parse a Query -> SELECT SelectList FROM FromList SEMICOLON."""
        tree = ParseTree()
        tree.root = "Azmi"
        self.match(Token.SELECT)
        tree.add_child(Node(Token.SELECT))

        tree.add_child(self.SelectList())
        
        self.match(Token.FROM)
        tree.add_child(Node(Token.FROM))
        tree.add_child(self.FromList())

        self.match(Token.SEMICOLON)
        tree.add_child(Node(Token.SEMICOLON))
        return tree

    def Field(self) -> ParseTree:
        print("Field")
        tree = ParseTree()

        """Parse a Field -> ATTRIBUTE | TABLE DOT ATTRIBUTE."""
        if self.current_token()[0] == Token.ATTRIBUTE:
            self.match(Token.ATTRIBUTE)
            tree.add_child(Node(Token.ATTRIBUTE))
            return tree
        elif self.current_token()[0] == Token.TABLE:
            self.match(Token.TABLE)
            tree.add_child(Node(Token.TABLE))
            self.match(Token.DOT)
            tree.add_child(Node(Token.DOT))
            self.match(Token.ATTRIBUTE)
            tree.add_child(Node(Token.ATTRIBUTE))
            return tree
        else:
            raise SyntaxError(f"Expected ATTRIBUTE or TABLE DOT ATTRIBUTE, found {self.current_token()[0]}")


    def SelectList(self) -> ParseTree:
        print("SelectList")
        """Parse a SelectList -> Field SelectList_."""
        tree = ParseTree()

        tree.add_child(self.Field())
        tree.add_child(self.SelectList_())

        return tree

    def SelectList_(self) -> ParseTree | None:
        print("SelectList_")
        """Parse SelectList_ -> COMMA Field SelectList_ | e."""
        if self.current_token() is None:
            return None
        
        tree = ParseTree()
        if self.current_token()[0] == Token.COMMA:
            self.match(Token.COMMA)
            tree.add_child(Node(Token.COMMA))
            tree.add_child(self.Field())
            tree.add_child(self.SelectList_())
            return tree

    def TableReference(self) -> ParseTree:
        print("TableReference")
        """Parse TableReference -> TABLE (AS TABLE)."""
        tree = ParseTree()
        self.match(Token.TABLE)
        tree.add_child(Node(Token.TABLE))
        if self.current_token() is None:
            return tree
        
        if self.current_token()[0] == Token.AS:
            self.match(Token.AS)
            tree.add_child(Node(Token.AS))
            self.match(Token.TABLE)
            tree.add_child(Node(Token.TABLE))
        
        return tree

    def FromList(self) -> ParseTree:
        print("FromList")
        """Parse FromList -> TableReference FromList_."""
        tree = ParseTree()
        tree.add_child(self.TableReference())
        tree.add_child(self.FromList_())
        return tree

    def FromList_(self) -> ParseTree | None:
        print("FromList_")
        """Parse FromList_ -> COMMA TableReference FromList_ | e."""
        if self.current_token() is None:
            return
        tree = ParseTree()

        if self.current_token()[0] == Token.COMMA:
            self.match(Token.COMMA)
            tree.add_child(Node(Token.COMMA))
            tree.add_child(self.TableReference())
            tree.add_child(self.FromList_())
            return tree

def parse(query_string) -> ParseTree:
    tokens = Lexer(query_string).tokenize()

    SQL_grammar = SQLGrammar(tokens)
    parse_tree = SQL_grammar.Query()
    return parse_tree