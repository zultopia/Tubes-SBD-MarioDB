from typing import Generic, TypeVar, List
from QueryOptimizer.data import QOData

T = TypeVar('T')
U = TypeVar('U')


"""
This class is based on the Prototype design pattern.
"""
class Prototype:
    def clone(self):
        raise NotImplementedError

"""
This is a pair just like in C++.
use generic type for the first and second element.
"""
class Pair(Generic[T, U], Prototype):
    def __init__(self, first: T, second: U):
        self.first = first
        self.second = second
    
    def clone(self):
        first: T = self.first
        if isinstance(first, Prototype):
            first = first.clone()
        
        second: U = self.second
        if isinstance(second, Prototype):
            second = second.clone()
        
        return Pair(first, second)
    
    def __repr__(self):
        return f"({self.first}, {self.second})"

