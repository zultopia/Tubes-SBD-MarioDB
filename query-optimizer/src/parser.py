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

    def match(self, tree: ParseTree, expected_token: Token):
        if self.current_token()[0] == expected_token:
            tree.add_child(Node(self.current_token()[0], self.current_token()[1]))
            self.pos += 1
            
        else:
            raise SyntaxError(f"Expected {expected_token}, found {self.current_token()}")

    def Query(self) -> ParseTree:
        """Parse a Query -> SELECT SelectList FROM FromList SEMICOLON."""
        tree = ParseTree()
        tree.root = "Query"
        self.match(tree, Token.SELECT)

        tree.add_child(self.SelectList())
        
        self.match(tree, Token.FROM)
        tree.add_child(self.FromList())

        self.match(tree, Token.SEMICOLON)
        return tree

    def Field(self) -> ParseTree:
        tree = ParseTree()
        tree.root = "Field"

        """Parse a Field -> ATTRIBUTE | TABLE DOT ATTRIBUTE."""
        if self.current_token()[0] == Token.ATTRIBUTE:
            self.match(tree, Token.ATTRIBUTE)
            return tree
        elif self.current_token()[0] == Token.TABLE:
            self.match(tree, Token.TABLE)
            self.match(tree, Token.DOT)
            self.match(tree, Token.ATTRIBUTE)
            return tree
        else:
            raise SyntaxError(f"Expected ATTRIBUTE or TABLE DOT ATTRIBUTE, found {self.current_token()[0]}")


    def SelectList(self) -> ParseTree:
        """Parse a SelectList -> Field SelectList_."""
        tree = ParseTree()
        tree.root = "SelectList"

        tree.add_child(self.Field())
        tree.add_child(self.SelectList_())

        return tree

    def SelectList_(self) -> ParseTree | None:
        """Parse SelectList_ -> COMMA Field SelectList_ | e."""
        if self.current_token() is None:
            return None
        
        tree = ParseTree()
        tree.root = "SelectList_"
        if self.current_token()[0] == Token.COMMA:
            self.match(tree, Token.COMMA)
            tree.add_child(self.Field())
            tree.add_child(self.SelectList_())
            return tree

    def TableReference(self) -> ParseTree:
        """Parse TableReference -> TABLE (AS TABLE)."""
        tree = ParseTree()
        tree.root = "TableReference"
        self.match(tree, Token.TABLE)
        if self.current_token() is None:
            return tree
        
        if self.current_token()[0] == Token.AS:
            self.match(tree, Token.AS)
            self.match(tree, Token.TABLE)
        
        return tree

    def FromList(self) -> ParseTree:
        """Parse FromList -> TableReference FromList_."""
        tree = ParseTree()
        tree.root = "FromList"
        tree.add_child(self.TableReference())
        tree.add_child(self.FromList_())
        return tree

    def FromList_(self) -> ParseTree | None:
        """Parse FromList_ -> COMMA TableReference FromList_ | e."""
        if self.current_token() is None:
            return
        tree = ParseTree()
        tree.root = "FromList_"

        if self.current_token()[0] == Token.COMMA:
            self.match(tree, Token.COMMA)
            tree.add_child(self.TableReference())
            tree.add_child(self.FromList_())
            return tree

def parse(query_string) -> ParseTree:
    tokens = Lexer(query_string).tokenize()

    SQL_grammar = SQLGrammar(tokens)
    parse_tree = SQL_grammar.Query()
    return parse_tree