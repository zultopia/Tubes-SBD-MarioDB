import pytest
from src.lexer import Lexer, Token

def test_tokenize():
    assert Lexer("select name from person;").tokenize() == [
        (Token.SELECT, 'select'), (Token.ATTRIBUTE, 'name'), (Token.FROM, 'from'),
        (Token.TABLE, 'person')
    ]