from typing import Generic, TypeVar

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
class Pair(Generic[T, U]):
    def __init__(self, first: T, second: U):
        self.first = first
        self.second = second