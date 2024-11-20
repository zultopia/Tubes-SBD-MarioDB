import pytest
from src.lexer import Lexer, Token

def test_tokenize():
    assert Lexer("select name from person;").tokenize() == [
        (Token.SELECT, 'select'), (Token.ATTRIBUTE, 'name'),(Token.FROM, 'from'),
        (Token.TABLE, 'person'),(Token.SEMICOLON, ';')
    ]

    assert Lexer("SELECT age1, prodi FROM user JOIN prodis ON user.prodiid = prodis.prodiid WHERE age1 > 20;").tokenize() ==[
    (Token.SELECT, 'SELECT'),
    (Token.ATTRIBUTE, 'age1'),
    (Token.COMMA, ','),
    (Token.ATTRIBUTE, 'prodi'),
    (Token.FROM, 'FROM'),
    (Token.TABLE, 'user'),
    (Token.JOIN, 'JOIN'),
    (Token.TABLE, 'prodis'),
    (Token.ON, 'ON'),
    (Token.ATTRIBUTE, 'user'),
    (Token.DOT, '.'),
    (Token.ATTRIBUTE, 'prodiid'),
    (Token.EQ, '='),
    (Token.ATTRIBUTE, 'prodis'),
    (Token.DOT, '.'),
    (Token.ATTRIBUTE, 'prodiid'),
    
    (Token.WHERE, 'WHERE'),
    (Token.ATTRIBUTE, 'age1'),
    (Token.GREATER, '>'),
    (Token.NUMBER, 20),
    (Token.SEMICOLON, ';')
    ]