from typing import Any, List, Tuple

from src.lexer import Token


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

    def Query(self):
        """Parse a Query -> SELECT SelectList FROM FromList SEMICOLON."""
        self.match(Token.SELECT)
        self.SelectList()
        self.match(Token.FROM)
        self.FromList()
        self.match(Token.SEMICOLON)
        print("Query parsed successfully!")

    def Field(self):
        """Parse a Field -> ATTRIBUTE | TABLE DOT ATTRIBUTE."""
        if self.current_token()[0] == Token.ATTRIBUTE:
            self.match(Token.ATTRIBUTE)
        elif self.current_token()[0] == Token.TABLE:
            self.match(Token.TABLE)
            self.match(Token.DOT)
            self.match(Token.ATTRIBUTE)
        else:
            raise SyntaxError(f"Expected ATTRIBUTE or TABLE DOT ATTRIBUTE, found {self.current_token()[0]}")

    def SelectList(self):
        """Parse a SelectList -> Field SelectList_."""
        self.Field()
        self.SelectList_()

    def SelectList_(self):
        """Parse SelectList_ -> COMMA Field SelectList_ | e."""
        if self.current_token() is None:
            return
        
        if self.current_token()[0] == Token.COMMA:
            self.match(Token.COMMA)
            self.Field()
            self.SelectList_()

    def TableReference(self):
        """Parse TableReference -> TABLE (AS TABLE)."""
        self.match(Token.TABLE)
        if self.current_token() is None:
            return
        
        if self.current_token()[0] == Token.AS:
            self.match(Token.AS)
            self.match(Token.TABLE)

    def FromList(self):
        """Parse FromList -> TableReference FromList_."""
        self.TableReference()
        self.FromList_()

    def FromList_(self):
        """Parse FromList_ -> COMMA TableReference FromList_ | e."""
        if self.current_token() is None:
            return
        
        if self.current_token()[0] == Token.COMMA:
            self.match(Token.COMMA)
            self.TableReference()
            self.FromList_()

    def get_parse_tree():
        pass