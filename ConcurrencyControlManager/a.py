import time

class A:
    name: str
    
    def __init__(self, name: str):
        self.name = name
    
    def __hash__(self):
        return hash(self.name)
    
    def __eq__(self, other: 'A'):
        return self.name == other.name
    
class B:
    a: A
    
    def __init__(self, a: A):
        self.a = a
    
    def __hash__(self):
        return self.a.__hash__()


b = B(A("a"))
print(b.__hash__())
time.sleep(5)
print(b.__hash__())

set = {}
set[A("a")] = "lala"

set[A("b")] = "lili"
time.sleep(5)
set[A("b")] = "lili"

print(len(set))