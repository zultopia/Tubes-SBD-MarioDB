import os
import pickle
from typing import Union, List, Dict
import uuid as UUID

from FailureRecoveryManager.Buffer import Buffer

class BPlusBlock:
    def __init__(self, block_id: int, uuid: str):
        self.block_id = block_id
        self.uuid = uuid
        
class BPlusLeaf:
    def __init__(self, block_id: int, identifier: Union[str, int]):
        self.block_id = block_id
        self.identifier = identifier
    
class BPlusTreeNode:
    def __init__(self, parent: Union[None, BPlusBlock]=None, is_leaf=False, prev: Union[None, BPlusBlock]=None,
                 next: Union[None, BPlusBlock]=None, table='', column='', uuid: Union[str, None]=None):
        self.uuid = str(UUID.uuid4()) if uuid is None else uuid
        self.table = table
        self.column = column
        self.prev = prev
        self.next = next
        self.is_leaf = is_leaf
        self.keys: List[Union[str, int]] = []
        self.children: List[BPlusBlock | BPlusLeaf] = []
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
        BPlusReader._update_bplus_block(self.table, self.column, self.block_id, self.uuid, self)
    
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
            if prev:
                BPlusReader._update_bplus_block(self.table, self.column, prev.block_id, prev.uuid, prev)
            return
        parent = BPlusReader._resolve_node(self.table, self.column, self.parent.block_id, self.parent.uuid)
        self_index = parent._find(self.keys[0])
        if self_index < len(parent.keys):
            next = parent.children[self_index + 1]
            next = BPlusReader._resolve_node(self.table, self.column, next.block_id, next.uuid)
            next.keys = self.keys + [parent.keys[self_index]] + next.keys
            for child_identifier in self.children:
                child = BPlusReader._resolve_node(self.table, self.column, child_identifier.block_id, child_identifier.uuid)
                child.parent = BPlusBlock(next.block_id, next.uuid)
                BPlusReader._update_bplus_block(self.table, self.column, child.block_id, child.uuid, child)
            next.children = self.children + next.children
            BPlusReader._update_bplus_block(self.table, self.column, next.block_id, next.uuid, next)
            return
        prev = parent.children[-2]
        prev = BPlusReader._resolve_node(self.table, self.column, prev.block_id, prev.uuid)
        prev.keys += [parent.keys[-1]] + self.keys
        for child_identifier in self.children:
            child = BPlusReader._resolve_node(self.table, self.column, child_identifier.block_id, child_identifier.uuid)
            child.parent = BPlusBlock(prev.block_id, prev.uuid)
            BPlusReader._update_bplus_block(self.table, self.column, child.block_id, child.uuid, child)
        prev.children += self.children
        BPlusReader._update_bplus_block(self.table, self.column, prev.block_id, prev.uuid, prev)
            
    def take(self, min: int):
        parent = None if not self.parent else BPlusReader._resolve_node(self.table, self.column, self.parent.block_id, self.parent.uuid)
        next = None if not self.next else BPlusReader._resolve_node(self.table, self.column, self.next.block_id, self.next.uuid)
        prev = None if not self.prev else BPlusReader._resolve_node(self.table, self.column, self.prev.block_id, self.prev.uuid)
        self_index = parent.get(self.keys[0])
        if self.is_leaf and self_index < len(parent.keys) and \
            len(next.keys) > min:
            self.keys += [next.keys.pop(0)]
            self.children += [next.children.pop(0)]
            parent.keys[self_index] = next.keys[0]
            BPlusReader._update_bplus_block(self.table, self.column, parent.block_id, parent.uuid, parent)
            BPlusReader._update_bplus_block(self.table, self.column, next.block_id, next.uuid, next)
            BPlusReader._update_bplus_block(self.table, self.column, self.block_id, self.uuid, self)
            return True
        elif self.is_leaf and self_index != 0 and len(prev.keys) > min:
            self.keys = [prev.keys.pop()] + self.keys
            self.children = [prev.children.pop()] + self.children
            parent.keys[self_index - 1] = self.keys[0]
            BPlusReader._update_bplus_block(self.table, self.column, parent.block_id, parent.uuid, parent)
            BPlusReader._update_bplus_block(self.table, self.column, prev.block_id, prev.uuid, prev)
            BPlusReader._update_bplus_block(self.table, self.column, self.block_id, self.uuid, self)
            return True
        if self.is_leaf or (self_index >= len(parent.keys) and self_index != 0):
            return False
        if self_index < len(parent.keys):
            next: BPlusBlock = parent.children[self_index + 1]
            next: BPlusTreeNode = BPlusReader._resolve_node(self.table, self.column, next.block_id, next.uuid)
            if len(next.keys) > min:
                self.keys += [parent.keys[self_index]]
                key_taken = next.children.pop(0)
                key_taken = BPlusReader._resolve_node(self.table, self.column, key_taken.block_id, key_taken.uuid)
                key_taken.parent = self
                self.children += [BPlusBlock(key_taken.block_id, key_taken.uuid)]
                parent.keys[self_index] = next.keys.pop(0)
                BPlusReader._update_bplus_block(self.table, self.column, next.block_id, next.uuid, next)
                BPlusReader._update_bplus_block(self.table, self.column, key_taken.block_id, key_taken.uuid, key_taken)
                BPlusReader._update_bplus_block(self.table, self.column, self.block_id, self.uuid, self)
        elif self_index != 0:
            prev: BPlusTreeNode = parent.children[self_index - 1]
            if len(prev.keys) > min:
                self.keys = [parent.keys[self_index - 1]] + self.keys
                key_taken = prev.children.pop()
                key_taken = BPlusReader._resolve_node(self.table, self.column, key_taken.block_id, key_taken.uuid)
                key_taken.parent = BPlusBlock(self.block_id, self.uuid)
                self.children = [BPlusBlock(key_taken.block_id, key_taken.uuid)] + self.children
                parent.keys[self_index - 1] = prev.keys.pop()
                BPlusReader._update_bplus_block(self.table, self.column, prev.block_id, prev.uuid, prev)
                BPlusReader._update_bplus_block(self.table, self.column, key_taken.block_id, key_taken.uuid, key_taken)
                BPlusReader._update_bplus_block(self.table, self.column, self.block_id, self.uuid, self)
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
    BUFFER: Union[None, Buffer] = None
    
    
    @staticmethod
    def read_block(table: str, block_id: str):
        block = BPlusReader.BUFFER.get_buffer(table, block_id)
        if not block:
            return
    
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
    def _resolve_node(table: str, column: str, block_id: int, uuid: str) -> BPlusTreeNode:
        block = BPlusReader.read_bplus_block(table, column, block_id)
        for node in block:
            if node.uuid == uuid:
                return node
        return None
    
    @staticmethod
    def _resolve_leaf(table: str, column: str, block_id: int, identifier: Union[str, int]) -> Dict:
        block = BPlusReader.read_block(table, column, block_id)
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
    def change_config(DATA_DIR="data_blocks/", BPLUS_DIR="bplus/", BUFFER=None):
        BPlusReader.DATA_DIR = DATA_DIR
        BPlusReader.BPLUS_DIR = BPLUS_DIR
        BPlusReader.BUFFER = BUFFER

class BPlusTree:
    DATA_DIR = "data_blocks/"
    HASH_DIR = "hash/"
    BPLUS_DIR = "bplus/"
    DEGREE = 8
    DEGREE_MIN = 4
    
    @staticmethod
    def initialize_bplus(table, column):
        root = BPlusTreeNode(is_leaf=True, parent=None, table=table, column=column, uuid='0')
        BPlusReader._add_bplus_block(table, column, root)

    @staticmethod
    def find(table: str, column: str, key) -> BPlusTreeNode:
        node = BPlusReader._resolve_node(table, column, 0, '0')
        while not node.is_leaf:
            node = node.get(key)
            node = BPlusReader._resolve_node(table, column, node.block_id, node.uuid)
        return node
    
    def __init__(self, table, column, degree):
        self.degree = max(degree, 2)
        self.degree_min = self.degree // 2
        self.table = table
        self.column = column
        self.root = BPlusTreeNode(is_leaf=True, table=self.table, column=self.column)

    @staticmethod
    def get(table: str, column: str, key):
        leaf = BPlusTree.find(table, column, key)
        for i, item in enumerate(leaf.keys):
            if item == key:
                return BPlusReader._resolve_leaf(table, column, leaf.children[i].identifier)
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
        