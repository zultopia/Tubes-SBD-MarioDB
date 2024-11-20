import pytest
from src.lexer import Lexer, Token

def test_tokenize():
    assert Lexer("select name from person;").tokenize() == [
        (Token.SELECT, 'select'), (Token.ATTRIBUTE, 'name'),(Token.FROM, 'from'),
        (Token.TABLE, 'person'),(Token.SEMICOLON, ';')
    ]

def test_select_multiple_attributes():
    assert Lexer("SELECT age1, prodi FROM user;").tokenize() == [
        (Token.SELECT, 'SELECT'),
        (Token.ATTRIBUTE, 'age1'),
        (Token.COMMA, ','),
        (Token.ATTRIBUTE, 'prodi'),
        (Token.FROM, 'FROM'),
        (Token.TABLE, 'user'),
        (Token.SEMICOLON, ';')
    ]

def test_select_with_join():
    assert Lexer("SELECT age1, prodi FROM user JOIN prodis ON user.prodiid = prodis.prodiid;").tokenize() == [
        (Token.SELECT, 'SELECT'),
        (Token.ATTRIBUTE, 'age1'),
        (Token.COMMA, ','),
        (Token.ATTRIBUTE, 'prodi'),
        (Token.FROM, 'FROM'),
        (Token.TABLE, 'user'),
        (Token.JOIN, 'JOIN'),
        (Token.TABLE, 'prodis'),
        (Token.ON, 'ON'),
        (Token.TABLE, 'user'),
        (Token.DOT, '.'),
        (Token.ATTRIBUTE, 'prodiid'),
        (Token.EQ, '='),
        (Token.TABLE, 'prodis'),
        (Token.DOT, '.'),
        (Token.ATTRIBUTE, 'prodiid'),
        (Token.SEMICOLON, ';')
    ]

def test_where_clause_with_float():
    assert Lexer("SELECT age1 FROM user WHERE age1 > 20.234;").tokenize() == [
        (Token.SELECT, 'SELECT'),
        (Token.ATTRIBUTE, 'age1'),
        (Token.FROM, 'FROM'),
        (Token.TABLE, 'user'),
        (Token.WHERE, 'WHERE'),
        (Token.ATTRIBUTE, 'age1'),
        (Token.GREATER, '>'),
        (Token.NUMBER, 20.234),
        (Token.SEMICOLON, ';')
    ]

def test_complex_query():
    assert Lexer("SELECT user.name, user.age FROM users JOIN addresses ON users.id AS uid = addresses.user_id aid WHERE user.age > 30;").tokenize() == [
        (Token.SELECT, 'SELECT'),
        (Token.TABLE, 'user'), (Token.DOT, '.'), (Token.ATTRIBUTE, 'name'), (Token.COMMA, ','),
        (Token.TABLE, 'user'), (Token.DOT, '.'), (Token.ATTRIBUTE, 'age'),
        (Token.FROM, 'FROM'),
        (Token.TABLE, 'users'),
        (Token.JOIN, 'JOIN'),
        (Token.TABLE, 'addresses'),
        (Token.ON, 'ON'),
        (Token.TABLE, 'users'), (Token.DOT, '.'), (Token.ATTRIBUTE, 'id'),
        (Token.AS, 'AS'), (Token.ATTRIBUTE, 'uid'),
        (Token.EQ, '='),
        (Token.TABLE, 'addresses'), (Token.DOT, '.'), (Token.ATTRIBUTE, 'user_id'),
        (Token.ATTRIBUTE, 'aid'),
        (Token.WHERE, 'WHERE'),
        (Token.TABLE, 'user'), 
        (Token.DOT, '.'),
        (Token.ATTRIBUTE, 'age'),
        (Token.GREATER, '>'),
        (Token.NUMBER, 30),
        (Token.SEMICOLON, ';')
    ]