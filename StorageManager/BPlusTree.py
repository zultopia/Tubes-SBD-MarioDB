from typing import List, Union, Dict

class Node(object):
    def __init__(self, parent: Union[None, 'Node']=None):
        self.keys: List = []
        self.values: List[Node] = []
        self.parent: Node = parent

    def __getitem__(self, item):
        return self.values[self.index(item)]

    def __setitem__(self, key, value: 'Node'):
        i = self.index(key)
        self.keys[i: i] = [key]
        self.values.pop(i)
        self.values[i: i] = value

    def __delitem__(self, key):
        i = self.index(key)
        del self.values[i]
        if i < len(self.keys):
            del self.keys[i]
        else:
            del self.keys[i - 1]
            
    def index(self, key):
        for i, item in enumerate(self.keys):
            if key < item:
                return i
        return len(self.keys)

    def split(self):
        left = Node(self.parent)
        mid = len(self.keys) // 2
        left.keys = self.keys[: mid]
        left.values = self.values[: mid + 1]
        for child in left.values:
            child.parent = left
        key = self.keys[mid]
        self.keys = self.keys[mid + 1:]
        self.values = self.values[mid + 1:]
        return key, [left, self]

    def fusion(self):
        index = self.parent.index(self.keys[0])
        if index < len(self.parent.keys):
            next_node: Node = self.parent.values[index + 1]
            next_node.keys[0: 0] = self.keys + [self.parent.keys[index]]
            for child in self.values:
                child.parent = next_node
            next_node.values[0: 0] = self.values
        else: 
            prev: Node = self.parent.values[-2]
            prev.keys += [self.parent.keys[-1]] + self.keys
            for child in self.values:
                child.parent = prev
            prev.values += self.values

    def borrow_key(self, minimum: int):
        index = self.parent.index(self.keys[0])
        if index < len(self.parent.keys):
            next_node: Node = self.parent.values[index + 1]
            if len(next_node.keys) > minimum:
                self.keys += [self.parent.keys[index]]
                borrow_node = next_node.values.pop(0)
                borrow_node.parent = self
                self.values += [borrow_node]
                self.parent.keys[index] = next_node.keys.pop(0)
                return True
        elif index != 0:
            prev: Node = self.parent.values[index - 1]
            if len(prev.keys) > minimum:
                self.keys[0: 0] = [self.parent.keys[index - 1]]
                borrow_node = prev.values.pop()
                borrow_node.parent = self
                self.values[0: 0] = [borrow_node]
                self.parent.keys[index - 1] = prev.keys.pop()
                return True
        return False
    
class Leaf(Node):
    def __init__(self, parent: Union[None, 'Node']=None, 
                 prev_node: Union[None, 'Node']=None, 
                 next_node: Union[None, 'Node']=None):
        super(Leaf, self).__init__(parent)
        self.next: Leaf = next_node
        if next_node is not None:
            next_node.prev = self
        self.prev: Leaf = prev_node
        if prev_node is not None:
            prev_node.next = self

    def __getitem__(self, item):
        return self.values[self.keys.index(item)]

    def __setitem__(self, key, value: Dict):
        i = self.index(key)
        if key not in self.keys:
            self.keys[i:i] = [key]
            self.values[i:i] = [value]
        else:
            self.values[i - 1] = value

    def __delitem__(self, key):
        i = self.keys.index(key)
        del self.keys[i]
        del self.values[i]
        
    def split(self):
        left = Leaf(self.parent, self.prev, self)
        mid = len(self.keys) // 2
        left.keys = self.keys[:mid]
        left.values = self.values[:mid]
        self.keys: list = self.keys[mid:]
        self.values: list = self.values[mid:]
        return self.keys[0], [left, self]

    def fusion(self):
        if self.next is not None and self.next.parent == self.parent:
            self.next.keys[0: 0] = self.keys
            self.next.values[0: 0] = self.values
        else:
            self.prev.keys += self.keys
            self.prev.values += self.values
        if self.next is not None:
            self.next.prev = self.prev
        if self.prev is not None:
            self.prev.next = self.next

    def borrow_key(self, minimum: int):
        index = self.parent.index(self.keys[0])
        if index < len(self.parent.keys) and len(self.next.keys) > minimum:
            self.keys += [self.next.keys.pop(0)]
            self.values += [self.next.values.pop(0)]
            self.parent.keys[index] = self.next.keys[0]
            return True
        elif index != 0 and len(self.prev.keys) > minimum:
            self.keys[0: 0] = [self.prev.keys.pop()]
            self.values[0: 0] = [self.prev.values.pop()]
            self.parent.keys[index - 1] = self.keys[0]
            return True
        return False
    
class BPlusTree(object):
    root: Node

    def __init__(self, maximum=4):
        self.root = Leaf()
        self.maximum: int = maximum if maximum > 2 else 2
        self.minimum: int = self.maximum // 2
        self.depth = 0

    def __getitem__(self, item):
        return self.find(item)[item]

    def __setitem__(self, key, value: Dict, leaf: Union[None, Leaf]=None):
        if leaf is None:
            leaf = self.find(key)
        leaf[key] = value
        if len(leaf.keys) > self.maximum:
            self.insert_index(*leaf.split())
            
    def find(self, key) -> Leaf:
        node = self.root
        while type(node) is not Leaf:
            node = node[key]
        return node

    def query(self, key):
        leaf = self.find(key)
        return leaf[key] if key in leaf.keys else None

    def change(self, key, value: Dict):
        leaf = self.find(key)
        if key not in leaf.keys:
            return False, leaf
        else:
            leaf[key] = value
            return True, leaf

    def insert(self, key, value: Dict):
        leaf = self.find(key)
        if key in leaf.keys:
            return False, leaf
        else:
            self.__setitem__(key, value, leaf)
            return True, leaf

    def insert_index(self, key, values: list[Node]):
        parent = values[1].parent
        if parent is None:
            values[0].parent = values[1].parent = self.root = Node()
            self.depth += 1
            self.root.keys = [key]
            self.root.values = values
            return
        parent[key] = values
        if len(parent.keys) > self.maximum:
            self.insert_index(*parent.split())

    def delete(self, key, node: Node = None):
        if node is None:
            node = self.find(key)
        del node[key]
        if len(node.keys) < self.minimum:
            if node == self.root:
                if len(self.root.keys) == 0 and len(self.root.values) > 0:
                    self.root = self.root.values[0]
                    self.root.parent = None
                    self.depth -= 1
                return
            elif not node.borrow_key(self.minimum):
                node.fusion()
                self.delete(key, node.parent)
    
    def range_query(self, minimum, maximum):
        leaf = self.find(minimum)
        stop = False
        query_result = []
        while leaf is not None and not stop:
            for i, key in enumerate(leaf.keys):
                if minimum <= key <= maximum:
                    query_result += leaf.values[i]
                elif key > maximum:
                    stop = True
                    break
            leaf = leaf.next
        return query_result
        
