# Query



Query -> UPDATE TABLE (AS TABLE) SET SetList (WHERE Condition) SEMICOLON | SELECT SelectList FROM FromList (WHERE Condition) (ORDER_BY Field (DESC | ASC)) (LIMIT NUMBER) SEMICOLON

# Update Set list

SetList -> SetTerm SetListTail
SetListTail -> COMMA SetTerm SetListTail | e
SetTerm -> [Field | NUMBER | STRING] EQ [Field | NUMBER | STRING]


# Select list
Field -> ATTRIBUTE | TABLE DOT ATTRIBUTE
SelectList -> Field SelectListTail | *
SelectListTail -> COMMA Field SelectListTail | e

# From list (including join)

TableResult -> TableTerm TableResultTail

TableResultTail -> NATURAL JOIN TableTerm TableResultTail
                 | JOIN TableTerm ON Condition TableResultTail
                 | e

TableTerm -> TABLE (AS TABLE)
           | OPEN_PARANTHESIS TableResult CLOSE_PARANTHESIS

FromList -> TableResult FromListTail

FromListTail -> COMMA TableResult FromListTail
              | e


# Where clause
Condition -> AndCondition ConditionTail
ConditionTail -> OR AndCondition ConditionTail | ε
AndCondition -> ConditionTerm AndConditionTail
AndConditionTail -> AND ConditionTerm AndConditionTail | ε
ConditionTerm -> [Field | NUMBER | STRING] ComparisonOperator [Field | NUMBER | STRING]
               | OPEN_PARANTHESIS Condition CLOSE_PARANTHESIS
               | NOT ConditionTerm
ComparisonOperator -> EQ | NEQ | GREATER | GREATER_EQ | LESS | LESS_EQ