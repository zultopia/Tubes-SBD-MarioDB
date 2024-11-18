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
    AND = 'AND'
    NOT = 'NOT'
    OR = 'OR'

    # Comparison operators
    GREATER = '>'
    GREATER_EQ = '>='
    LESS = '<'
    LESS_EQ = '<='
    EQ = '='


    # Others
    COMMA = ',' # buat nama tabel atau hasil aliasing table contoh 'SELECT ATTRIBUTE from TABLE1, TABLE2, TABLE3;'
    TABLE = None # buat nama attribute contoh 'select ATTRIBUTE from TABLE;' atau 'select TABLE.ATTRIBUTE from TABLE;;
    ATTRIBUTE = None
    DOT = '.'

    SEMICOLON = ';'

    # begin transaction dan commit tidak perlu karena kita hanya melakukan parsing query
