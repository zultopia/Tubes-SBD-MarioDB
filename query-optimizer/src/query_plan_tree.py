from typing import List
from src.parse_tree import Node, ParseTree

class Plan:
    # Ini terserah mau dibuat kayak gimana.
    # Yang jelas minimal harus ada:
    #   1. select: mengconsider index-index yang ada, operasi-operasi yang dilakukan
    #   2. sorting: external sort-merge
    #   3. join: (indexed) nested-loop, block nested-loop, merge, hybrid merge, hash.
    # Apakah ada yang kurang?
    pass

class QueryPlanTree:
    root: Node
    childs: List['QueryPlanTree'] = []
    
    plan: Plan | None


def from_parse_tree(self, parse_tree: ParseTree) -> QueryPlanTree:
    pass
    