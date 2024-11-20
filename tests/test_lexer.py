from typing import Any, List, Tuple
import pytest
from src.lexer import Lexer, Token

def test_tokenize():
    assert Lexer("select name from person;").tokenize() == [
        (Token.SELECT, 'select'), (Token.ATTRIBUTE, 'name'), (Token.FROM, 'from'),
        (Token.TABLE, 'person'), (Token.SEMICOLON, ';')
        ]


# While waiting for the implementation of the tokenize function, other codes can call this instead.
def mock_tokenize(query_string: str) -> List[Tuple[Token, Any]]:
    if query_string == "select name from person;":
        return [
        (Token.SELECT, 'select'), (Token.ATTRIBUTE, 'name'), (Token.FROM, 'from'),
        (Token.TABLE, 'person'), (Token.SEMICOLON, ';')
        ]
    else:
        print("Query string not recognized in the mock function")

