
# Implemented

Query -> SELECT SelectList FROM FromList SEMICOLON

Field -> ATTRIBUTE | TABLE DOT ATTRIBUTE
SelectList -> Field SelectList_
SelectList_ -> COMMA Field SelectList_ | e

TableReference -> TABLE (AS TABLE)
FromList -> TableReference FromList_
FromList_ -> COMMA TableReference FromList_ | e


# Not Implemented yet (productions are being added gradually!)

Query -> SELECT SelectList FROM FromList (WHERE Condition) (ORDER_BY Condition) (LIMIT NUMBER) SEMICOLON

Field -> ATTRIBUTE | TABLE DOT ATTRIBUTE
SelectList -> Field SelectList_
SelectList_ -> COMMA Field SelectList_ | e

TableReference -> TABLE (AS TABLE)
FromList -> TableReference FromList_
FromList_ -> COMMA TableReference FromList_ | e
