from typing import Any, List, Tuple
from QueryOptimizer.lexer import Token, Lexer
from QueryOptimizer.parse_tree import Node, ParseTree


class SQLGrammar:
    def __init__(self, tokens):
        self.tokens: List[Tuple[Token, Any]] = tokens
        self.pos = 0

    def current_token(self):
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def match(self, tree: ParseTree, expected_token: Token):
        if self.current_token()[0] == expected_token:
            tree.add_child(Node(self.current_token()[0], self.current_token()[1]))
            self.pos += 1
        else:
            raise SyntaxError(f"Expected {expected_token}, found {self.current_token()}")

    def Query(self) -> ParseTree:
        """Parse Query -> SELECT SelectList FROM FromList (WHERE Condition) (ORDER_BY Condition) (LIMIT NUMBER) SEMICOLON."""
        tree = ParseTree()
        tree.root = "Query"
        self.match(tree, Token.SELECT)
        tree.add_child(self.SelectList())
        self.match(tree, Token.FROM)
        tree.add_child(self.FromList())

        if self.current_token() and self.current_token()[0] == Token.WHERE:
            self.match(tree, Token.WHERE)
            tree.add_child(self.Condition())

        if self.current_token() and self.current_token()[0] == Token.ORDER_BY:
            self.match(tree, Token.ORDER_BY)
            tree.add_child(self.Condition())

        if self.current_token() and self.current_token()[0] == Token.LIMIT:
            self.match(tree, Token.LIMIT)
            self.match(tree, Token.NUMBER)

        self.match(tree, Token.SEMICOLON)
        return tree

    # Select List
    def Field(self) -> ParseTree:
        """Parse Field -> ATTRIBUTE | TABLE DOT ATTRIBUTE."""
        tree = ParseTree()
        tree.root = "Field"

        if self.current_token()[0] == Token.ATTRIBUTE:
            self.match(tree, Token.ATTRIBUTE)
        elif self.current_token()[0] == Token.TABLE:
            self.match(tree, Token.TABLE)
            self.match(tree, Token.DOT)
            self.match(tree, Token.ATTRIBUTE)
        else:
            raise SyntaxError(f"Expected ATTRIBUTE or TABLE DOT ATTRIBUTE, found {self.current_token()[0]}")

        return tree

    def SelectList(self) -> ParseTree:
        """Parse SelectList -> Field SelectListTail."""
        tree = ParseTree()
        tree.root = "SelectList"
        tree.add_child(self.Field())
        tree.add_child(self.SelectListTail())
        return tree

    def SelectListTail(self) -> ParseTree | None:
        """Parse SelectListTail -> COMMA Field SelectListTail | e."""
        if self.current_token() and self.current_token()[0] == Token.COMMA:
            tree = ParseTree()
            tree.root = "SelectListTail"
            self.match(tree, Token.COMMA)
            tree.add_child(self.Field())
            tree.add_child(self.SelectListTail())
            return tree
        return None

    # From List and Join
    def TableResult(self) -> ParseTree:
        """Parse TableResult -> TableTerm TableResultTail."""
        tree = ParseTree()
        tree.root = "TableResult"
        tree.add_child(self.TableTerm())
        tree.add_child(self.TableResultTail())
        return tree

    def TableResultTail(self) -> ParseTree | None:
        """Parse TableResultTail -> NATURAL JOIN TableTerm TableResultTail
                                   | JOIN TableTerm ON Condition TableResultTail
                                   | e."""
        if self.current_token() is None:
            return None

        tree = ParseTree()
        tree.root = "TableResultTail"

        if self.current_token()[0] == Token.NATURAL:
            self.match(tree, Token.NATURAL)
            self.match(tree, Token.JOIN)
            tree.add_child(self.TableTerm())
            tree.add_child(self.TableResultTail())
        elif self.current_token()[0] == Token.JOIN:
            self.match(tree, Token.JOIN)
            tree.add_child(self.TableTerm())
            self.match(tree, Token.ON)
            tree.add_child(self.Condition())
            tree.add_child(self.TableResultTail())
        else:
            return None

        return tree

    def TableTerm(self) -> ParseTree:
        """Parse TableTerm -> TABLE (AS TABLE) | OPEN_PARANTHESIS TableResult CLOSE_PARANTHESIS."""
        tree = ParseTree()
        tree.root = "TableTerm"

        if self.current_token()[0] == Token.TABLE:
            self.match(tree, Token.TABLE)
            if self.current_token() and self.current_token()[0] == Token.AS:
                self.match(tree, Token.AS)
                self.match(tree, Token.TABLE)
        elif self.current_token()[0] == Token.OPEN_PARANTHESIS:
            self.match(tree, Token.OPEN_PARANTHESIS)
            tree.add_child(self.TableResult())
            self.match(tree, Token.CLOSE_PARANTHESIS)
        else:
            raise SyntaxError(f"Expected TABLE or OPEN_PARANTHESIS, found {self.current_token()[0]}")

        return tree

    def FromList(self) -> ParseTree:
        """Parse FromList -> TableResult FromListTail."""
        tree = ParseTree()
        tree.root = "FromList"
        tree.add_child(self.TableResult())
        tree.add_child(self.FromListTail())
        return tree

    def FromListTail(self) -> ParseTree | None:
        """Parse FromListTail -> COMMA TableResult FromListTail | e."""
        if self.current_token() and self.current_token()[0] == Token.COMMA:
            tree = ParseTree()
            tree.root = "FromListTail"
            self.match(tree, Token.COMMA)
            tree.add_child(self.TableResult())
            tree.add_child(self.FromListTail())
            return tree
        return None

    # Where Clause
    def Condition(self) -> ParseTree:
        """Parse Condition -> AndCondition ConditionTail."""
        tree = ParseTree()
        tree.root = "Condition"
        tree.add_child(self.AndCondition())
        tree.add_child(self.ConditionTail())
        return tree

    def ConditionTail(self) -> ParseTree | None:
        """Parse ConditionTail -> OR AndCondition ConditionTail | e."""
        if self.current_token() and self.current_token()[0] == Token.OR:
            tree = ParseTree()
            tree.root = "ConditionTail"
            self.match(tree, Token.OR)
            tree.add_child(self.AndCondition())
            tree.add_child(self.ConditionTail())
            return tree
        return None

    def AndCondition(self) -> ParseTree:
        """Parse AndCondition -> ConditionTerm AndConditionTail."""
        tree = ParseTree()
        tree.root = "AndCondition"
        tree.add_child(self.ConditionTerm())
        tree.add_child(self.AndConditionTail())
        return tree

    def AndConditionTail(self) -> ParseTree | None:
        """Parse AndConditionTail -> AND ConditionTerm AndConditionTail | e."""
        if self.current_token() and self.current_token()[0] == Token.AND:
            tree = ParseTree()
            tree.root = "AndConditionTail"
            self.match(tree, Token.AND)
            tree.add_child(self.ConditionTerm())
            tree.add_child(self.AndConditionTail())
            return tree
        return None

    def ConditionTerm(self) -> ParseTree:
        """Parse ConditionTerm -> [Field | NUMBER | STRING] ComparisonOperator [Field | NUMBER | STRING]
                                 | OPEN_PARANTHESIS Condition CLOSE_PARANTHESIS
                                 | NOT ConditionTerm."""
        tree = ParseTree()
        tree.root = "ConditionTerm"

        if self.current_token()[0] == Token.NOT:
            self.match(tree, Token.NOT)
            tree.add_child(self.ConditionTerm())
        elif self.current_token()[0] == Token.OPEN_PARANTHESIS:
            self.match(tree, Token.OPEN_PARANTHESIS)
            tree.add_child(self.Condition())
            self.match(tree, Token.CLOSE_PARANTHESIS)
        else:
            if self.current_token()[0] == Token.NUMBER:
                self.match(tree, Token.NUMBER)
            elif self.current_token()[0] == Token.STRING:
                self.match(tree, Token.STRING)
            else:
                tree.add_child(self.Field())
        
            tree.add_child(self.ComparisonOperator())
            
            if self.current_token()[0] == Token.NUMBER:
                self.match(tree, Token.NUMBER)
            elif self.current_token()[0] == Token.STRING:
                self.match(tree, Token.STRING)
            else:
                tree.add_child(self.Field())

        return tree

    def ComparisonOperator(self) -> ParseTree:
        """Parse ComparisonOperator -> EQ | NEQ | GREATER | GREATER_EQ | LESS | LESS_EQ."""
        tree = ParseTree()
        tree.root = "ComparisonOperator"
        valid_tokens = {Token.EQ, Token.NEQ, Token.GREATER, Token.GREATER_EQ, Token.LESS, Token.LESS_EQ}

        if self.current_token()[0] in valid_tokens:
            self.match(tree, self.current_token()[0])
        else:
            raise SyntaxError(f"Expected a comparison operator, found {self.current_token()[0]}")

        return tree


def parse(query_string) -> ParseTree:
    tokens = Lexer(query_string).tokenize()

    SQL_grammar = SQLGrammar(tokens)
    parse_tree = SQL_grammar.Query()
    return parse_tree