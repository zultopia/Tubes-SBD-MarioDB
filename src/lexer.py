from enum import Enum
import re
from typing import Any, List, Tuple

class Token(Enum):
    # Basic SQL keywords
    SELECT = 'select'
    UPDATE = 'update'
    FROM = 'from'
    AS = 'as'
    JOIN = 'join'
    WHERE = 'where'
    ORDER_BY = 'order by'
    LIMIT = 'limit'
    ON = 'on'
    NATURAL = 'natural'

    # Logical operators
    AND = 'and'
    NOT = 'not'
    OR = 'or'

    # Comparison operators
    GREATER = '>'
    GREATER_EQ = '>='
    LESS = '<'
    LESS_EQ = '<='
    EQ = '='
    NEQ = '<>'

    # Literals
    NUMBER = 1 # Contoh: 3.14 atau 19
    STRING = 2 # 'John' (btw ini termasuk single quotation marksnya ya)

    # Others
    ASTERISK = '*'
    COMMA = ','
    TABLE = 3 # buat nama tabel atau hasil aliasing table contoh 'SELECT ATTRIBUTE from TABLE1, TABLE2, TABLE3;' 
    ATTRIBUTE = 4 # buat nama attribute contoh 'select ATTRIBUTE from TABLE;' atau 'select TABLE.ATTRIBUTE from TABLE;'
    DOT = '.'
    OPEN_PARANTHESIS = '('
    CLOSE_PARANTHESIS = ')'
    ASC = 'ascending'
    DES = 'descending'
    WS = None # tokens white spaces

    SEMICOLON = ';'

class Lexer:
    # assumption:
    # query is in a single line
    query_string: str
    def __init__(self, query_string):
        #explanation on state:
        # 0:expecting attribute, used for after SELECT,DOT
        # 1:expecting table, used for after FROM   
        self.state = 0
        self.query_string = query_string
        self.regex_patterns = [
    #some notes:
    #(?i) means case insensitive

    # Basic SQL keywords
    (Token.SELECT, r"(?i)\bselect\b"),   
    (Token.UPDATE, r"(?i)\bupdate\b"),
    (Token.FROM, r"(?i)\bfrom\b"),
    (Token.AS, r"(?i)\bas\b"),
    (Token.JOIN, r"(?i)\bjoin\b"),
    (Token.WHERE, r"(?i)\bwhere\b"),
    (Token.ORDER_BY, r"(?i)\border\s+by\b"),
    (Token.LIMIT, r"(?i)\blimit\b"),
    (Token.ON, r"(?i)\bon\b"),
    (Token.NATURAL, r"(?i)\bnatural\b"),

    # Logical operators
    (Token.AND, r"(?i)\band\b"),
    (Token.NOT, r"(?i)\bnot\b"),
    (Token.OR, r"(?i)\bor\b"),

    # Comparison operators
    (Token.GREATER, r">"),      
    (Token.GREATER_EQ, r">="),   
    (Token.LESS, r"<"),         
    (Token.LESS_EQ, r"<="),      
    (Token.EQ, r"="),           
    (Token.NEQ, r"<>"),          

    # Literals
    (Token.NUMBER, r"\b\d+(\.\d+)?\b"),  
    (Token.STRING, r"""(['"])(?:(?=(\\?))\2.)*?\1"""),     

    # Others
    (Token.ASTERISK, r"\*"),
    (Token.COMMA, r","),    
    (Token.TABLE, r"[a-zA-Z_][a-zA-Z0-9_]*"),
    (Token.ATTRIBUTE, r"[a-zA-Z_][a-zA-Z0-9_]*"),   
    (Token.DOT, r"\."),                  
    (Token.OPEN_PARANTHESIS, r"\("),     
    (Token.CLOSE_PARANTHESIS, r"\)"),    
    (Token.ASC, r"(?i)\bascending\b"),   
    (Token.DES, r"(?i)\bdescending\b"),           

    (Token.SEMICOLON, r";")              
]
    def tokenize(self) -> List[Tuple[Token, Any]]:
        result = []
        tempQuery = self.query_string
        while len(tempQuery) > 0:
            isMatch = None
            for tokenExpr in self.regex_patterns:
                token, regex = tokenExpr
                isMatch = re.match(regex,tempQuery)
                if isMatch:
                    tokenValue = token.value
                    actualValue = isMatch.group()
                    avLength = len(actualValue)
                    if type(tokenValue) == int:
                        if token == Token.NUMBER:
                            result.append((token, float(actualValue)))
                        elif token in {Token.ATTRIBUTE, Token.TABLE}:
                            if self.state == 0:
                                result.append((Token.ATTRIBUTE, actualValue))
                            else:
                                result.append((Token.TABLE, actualValue))
                        else:
                            result.append((token, actualValue))
                    else:
                        if token == Token.SELECT or token == Token.DOT:
                            self.state = 0
                        elif token == Token.FROM:
                            self.state = 1
                        result.append((token,actualValue))
                    tempQuery = tempQuery[avLength:].lstrip()
                    break # so that if match is found, y'don't have to loop through the entire thing
            if isMatch is None: #if no patterns match return None
                return


        self.state=0
        return result
    
print(Lexer("SELECT age1, prodi FROM user JOIN prodis ON user.prodiid = prodis.prodiid WHERE age1 > 20;").tokenize())