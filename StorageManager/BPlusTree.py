import os
import pickle
from typing import Union, List, Dict
import uuid

class BPlusBlock:
    def __init__(self, block_id: int, uuid: str):
        self.block_id = block_id
        self.uuid = uuid
    
class BPlusTreeNode:
    def __init__(self, parent: Union[None, BPlusBlock]=None, is_leaf=False, prev: Union[None, BPlusBlock]=None,
                 next: Union[None, BPlusBlock]=None, table='', column=''):
        self.uuid = str(uuid.uuid4())
        self.table = table
        self.column = column
        self.prev = prev
        self.next = next
        self.is_leaf = is_leaf
        self.keys: List[Union[str, int]] = []
        self.children: List[BPlusBlock] = []
        self.parent: BPlusBlock = parent
        if self.is_leaf and self.prev:
            prev_node = BPlusReader._resolve_node(table, column, prev.block_id, prev.uuid)
            prev_node.next = self
            BPlusReader._update_bplus_block(table, column, prev_node.block_id, prev_node.uuid, prev_node)
        if self.is_leaf and self.next:
            next_node = BPlusReader._resolve_node(table, column, prev.block_id, prev.uuid)
            next_node.prev = self
            BPlusReader._update_bplus_block(table, column, next_node.block_id, next_node.uuid, next_node)
        self.block_id = BPlusReader._add_bplus_block(table, column, self)
        
    def _find(self, key: Union[str, int]):
        for i, item in enumerate(self.keys):
            if key < item:
                return i
        return len(self.keys)

    def get(self, item: Union[str, int]):
        return self.children[self._find(item)]
    
    def delete(self, key):
        key_index = self._find(key)
        self.children.pop(key_index)
        if key_index < len(self.keys):
            self.keys.pop(key_index)
        else:
            self.keys.pop(key_index - 1)
    
    def merge(self):
        next = None if not self.next else BPlusReader._resolve_node(self.table, self.column, self.next.block_id, self.next.uuid)
        prev = None if not self.prev else BPlusReader._resolve_node(self.table, self.column, self.prev.block_id, self.prev.uuid)
        if self.is_leaf and next and next.parent == self.parent:
            next.keys = self.keys + next.keys
            next.children = self.children + next.children
        elif self.is_leaf:
            prev.keys += self.keys
            prev.children += self.children
        if self.is_leaf and self.next:
            next.prev = self.prev
        elif self.is_leaf and self.prev:
            prev.next = self.next
        if self.is_leaf:
            if next:
                BPlusReader._update_bplus_block(self.table, self.column, next.block_id, next.uuid, next)
            return
        self_index = self.parent._find(self.keys[0])
        if self_index < len(self.parent.keys):
            next = self.parent.children[self_index + 1]
            next.keys = self.keys + [self.parent.keys[self_index]] + next.keys
            for child in self.children:
                child.parent = next
            next.children = self.children + next.children
            return
        prev = self.parent.children[-2]
        prev.keys += [self.parent.keys[-1]] + self.keys
        for child in self.children:
            child.parent = prev
        prev.children += self.children
            
    def take(self, min: int):
        self_index = self.parent.get(self.keys[0])
        if self.is_leaf and self_index < len(self.parent.keys) and \
            len(self.next.keys) > min:
            self.keys += [self.next.keys.pop(0)]
            self.children += [self.next.children.pop(0)]
            self.parent.keys[self_index] = self.next.keys[0]
            return True
        elif self.is_leaf and self_index != 0 and len(self.prev.keys) > min:
            self.keys = [self.prev.keys.pop()] + self.keys
            self.children = [self.prev.children.pop()] + self.children
            self.parent.keys[self_index - 1] = self.keys[0]
            return True
        if self.is_leaf or (self_index >= len(self.parent.keys) and self_index != 0):
            return False
        if self_index < len(self.parent.keys):
            next: BPlusTreeNode = self.parent.children[self_index + 1]
            if len(next.keys) > min:
                self.keys += [self.parent.keys[self_index]]
                key_taken = next.children.pop(0)
                key_taken.parent = self
                self.children += [key_taken]
                self.parent.keys[self_index] = next.keys.pop(0)
        elif self_index != 0:
            prev: BPlusTreeNode = self.parent.children[self_index - 1]
            if len(prev.keys) > min:
                self.keys = [self.parent.keys[self_index - 1]] + self.keys
                key_taken = prev.children.pop()
                key_taken.parent = self
                self.children = [key_taken] + self.children
                self.parent.keys[self_index - 1] = prev.keys.pop()
        return True
    
    def split(self):
        if self.is_leaf:
            left = BPlusTreeNode(self.parent, True, self.prev, BPlusBlock(self.block_id, self.uuid), self.table, self.column)
            mid = len(self.keys) // 2
            left.keys = self.keys[:mid]
            left.children = self.children[:mid]
            self.keys = self.keys[mid:]
            self.children = self.children[mid:]
            BPlusReader._update_bplus_block(left.table, left.column, left.block_id, left.uuid, left)
            BPlusReader._update_bplus_block(self.table, self.column, self.block_id, self.uuid, self)
            return (self.keys[0], [left, self])
        left = BPlusTreeNode(self.parent, table=self.table, column=self.column)
        mid = len(self.keys) // 2
        left.keys = self.keys[:mid]
        left.children = self.children[:mid + 1]
        for child_block in left.children:
            child = BPlusReader._resolve_node(self.table, self.column, child_block.block_id, child_block.uuid)
            child.parent = BPlusBlock(left.block_id, left.uuid)
            BPlusReader._update_bplus_block(self.table, self.column, child.block_id, child.uuid, child)
        key = self.keys[mid]
        self.keys = self.keys[mid + 1:]
        self.children = self.children[mid + 1:]
        BPlusReader._update_bplus_block(left.table, left.column, left.block_id, left.uuid, left)
        BPlusReader._update_bplus_block(self.table, self.column, self.block_id, self.uuid, self)
        return (key, [left, self])
    
class BPlusReader:
    
    DATA_DIR = "data_blocks/"
    HASH_DIR = "hash/"
    BPLUS_DIR = "bplus/"
    BLOCK_SIZE = 4096
    
    @staticmethod
    def _get_bplus_block_file(table: str, column: str, block_id: int) -> str:
        return os.path.join(BPlusReader.DATA_DIR, BPlusReader.BPLUS_DIR, f"{table}__{column}__bplus__block__{block_id}.blk")
    
    @staticmethod
    def _load_bplus_block(table: str, column: str, block_id: int) -> List[Dict]:
        block_file = BPlusReader._get_bplus_block_file(table, column, block_id)
        if os.path.exists(block_file):
            with open(block_file, "rb") as file:
                return pickle.load(file)
        return []
            
    @staticmethod
    def read_bplus_block(table: str, column: str, block_id: int) -> List[BPlusTreeNode]:
        results = []
        block = BPlusReader._load_bplus_block(table, column, block_id)
        for row in block:
            results.append(row)
        return results
    
    @staticmethod
    def _save_bplus_block(table: str, column: str, block_id: int, block_data: List[Dict]):
        block_file = BPlusReader._get_bplus_block_file(table, column, block_id)
        if not block_data:
            if os.path.exists(block_file):
                os.remove(block_file)
                return
        with open(block_file, "wb") as file:
            pickle.dump(block_data, file)
    
    @staticmethod
    def _resolve_node(table: str, column: str, block_id: int, uuid: int) -> BPlusTreeNode:
        block = BPlusReader.read_bplus_block(table, column, block_id)
        for node in block:
            if node.uuid == uuid:
                return node
        return None
    
    @staticmethod
    def _update_bplus_block(table: str, column: str, block_id: int, uuid: str, updated_node: BPlusTreeNode):
        bplus_block = BPlusReader.read_bplus_block(table, column, block_id)
        new_block = []
        for node in bplus_block:
            if node.uuid != uuid:
                new_block.append(node)
        new_block.append(updated_node)
        BPlusReader._save_bplus_block(table, column, block_id, new_block)
    
    @staticmethod
    def _add_bplus_block(table: str, column: str, node: BPlusTreeNode) -> int:
        blocks = sorted(int(file.split('__')[-1].split('.')[0]) 
                        for file in os.listdir(os.path.join(BPlusReader.DATA_DIR, BPlusReader.BPLUS_DIR)) 
                        if file.startswith(f"{table}__{column}__bplus"))
        # add operation
        for block_id in blocks:
            block = BPlusReader.read_bplus_block(table, column, block_id)
            if len(block) < BPlusReader.BLOCK_SIZE:
                block.append(node)
                return block_id
        # If no space, create a new block
        new_block_id = max(blocks, default=-1) + 1
        new_block = [node]
        # give clue it exists
        BPlusReader._save_bplus_block(table, column, new_block_id, new_block)
        return new_block_id
    
    @staticmethod
    def change_config(DATA_DIR="data_blocks/", BPLUS_DIR="bplus/"):
        BPlusReader.DATA_DIR = DATA_DIR
        BPlusReader.BPLUS_DIR = BPLUS_DIR

class BPlusTree:
    def __init__(self, table, column, degree):
        self.degree = max(degree, 2)
        self.degree_min = self.degree // 2
        self.table = table
        self.column = column
        self.root = BPlusTreeNode(is_leaf=True, table=self.table, column=self.column)

    def find(self, key):
        node = self.root
        while not node.is_leaf:
            node = node.get(key)
        return node

    def get(self, key):
        leaf = self.find(key)
        for i, item in enumerate(leaf.keys):
            if item == key:
                return leaf.children[i]
        return None
    
    def set(self, key, value, leaf=None):
        if not leaf:
            leaf = self.find(key)
        

    def insert(self, key, value):
        leaf = self.find(key)
        if key in leaf.keys:
            raise ValueError(f"Key {key} tidak unik.")
        self._insert_in_leaf(leaf, key, value)
        if len(leaf.keys) == self.degree:
            self._split_node(leaf)

    def delete(self, key):
        leaf = self._find_leaf(self.root, key)
        if key in leaf.keys:
            index = leaf.keys.index(key)
            del leaf.keys[index]
            del leaf.children[index]
        else:
            raise KeyError(f"Key {key} tidak ditemukan.")

    def _insert_in_leaf(self, leaf: BPlusTreeNode, key, value):
        index = 0
        while index < len(leaf.keys) and key > leaf.keys[index]:
            index += 1
        leaf.keys.insert(index, key)
        leaf.children.insert(index, value)

    def _split_node(self, node: BPlusTreeNode):
        middle = len(node.keys) // 2
        sibling = BPlusTreeNode(is_leaf=node.is_leaf)
        sibling.keys = node.keys[middle:]
        sibling.children = node.children[middle:]
        node.keys = node.keys[:middle]
        node.children = node.children[:middle]

        if node == self.root:
            new_root = BPlusTreeNode()
            new_root.keys = [sibling.keys[0]]
            new_root.children = [node, sibling]
            self.root = new_root
        else:
            parent = self._find_parent(self.root, node)
            self._insert_in_internal(parent, sibling.keys[0], sibling)

    def _find_parent(self, current: BPlusTreeNode, child):
        if current.is_leaf or current.children[0].is_leaf:
            return None
        for node in current.children:
            if node == child:
                return current
            parent = self._find_parent(node, child)
            if parent:
                return parent
        return None

    def _insert_in_internal(self, parent: BPlusTreeNode, key, child):
        index = 0
        while index < len(parent.keys) and key > parent.keys[index]:
            index += 1
        parent.keys.insert(index, key)
        parent.children.insert(index + 1, child)
        if len(parent.keys) == self.degree:
            self._split_node(parent)