from enum import Enum

class Token(Enum):
    QUERY = None # Ini token_type dari rootnya parse trree  

    # Basic SQL keywords
    SELECT = 'select'
    UPDATE = 'update'
    FROM = 'from'
    AS = 'as'
    JOIN = 'join'
    WHERE = 'where'
    ORDER_BY = 'order by'
    LIMIT = 'limit'

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


    # Others
    COMMA = ','
    TABLE = None # buat nama tabel atau hasil aliasing table contoh 'SELECT ATTRIBUTE from TABLE1, TABLE2, TABLE3;' 
    ATTRIBUTE = None # buat nama attribute contoh 'select ATTRIBUTE from TABLE;' atau 'select TABLE.ATTRIBUTE from TABLE;'
    DOT = '.'
    OPEN_PARANTHESIS = '('
    CLOSE_PARANTHESIS = ')'
    QUOTE = '\''

    SEMICOLON = ';'
