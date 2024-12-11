import os
import pickle
from typing import Union, List, Dict

class BPlusTreeNode:
    def __init__(self, is_leaf=False):
        self.is_leaf = is_leaf
        self.keys = []
        self.children: List[Union[Dict, BPlusTreeNode]] = []

class BPlusTree:
    def __init__(self, degree):
        self.root = BPlusTreeNode(is_leaf=True)
        self.degree = degree

    def _find_leaf(self, node: BPlusTreeNode, key):
        if node.is_leaf:
            return node
        for i in range(len(node.keys)):
            if key < node.keys[i]:
                return self._find_leaf(node.children[i], key)
        return self._find_leaf(node.children[-1], key)

    def get(self, key):
        leaf = self._find_leaf(self.root, key)
        for i, item in enumerate(leaf.keys):
            if item == key:
                return leaf.children[i]
        return None

    def insert(self, key, value):
        leaf = self._find_leaf(self.root, key)
        if key in leaf.keys:
            raise ValueError(f"Key {key} tidak unik.")
        self._insert_in_leaf(leaf, key, value)
        if len(leaf.keys) == self.degree:
            self._split_node(leaf)

    def update(self, key, value):
        leaf = self._find_leaf(self.root, key)
        for i, item in enumerate(leaf.keys):
            if item == key:
                leaf.children[i] = value
                return
        raise KeyError(f"Key {key} tidak ditemukan.")

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
            
class BPlusReader:
    DATA_DIR = "data_blocks/"
    HASH_DIR = "hash/"
    BPLUS_DIR = "bplus/"
    
    """
    Bawah ini copasan StorageManager
    """
    @staticmethod
    def _get_block_file(table: str, block_id: int) -> str:
        return os.path.join(BPlusReader.DATA_DIR, f"{table}_block_{block_id}.blk")
    
    @staticmethod
    def _load_block(table: str, block_id: int) -> List[Dict]:
        block_file = Hash._get_block_file(table, block_id)
        if os.path.exists(block_file):
            with open(block_file, "rb") as file:
                return pickle.load(file)
        return []
    
    @staticmethod
    def read_block(table: str, column: str, hash_value: int, block_id: int):
        results = []
        block = Hash._load_block(table, block_id)
        for row in block:
            if Hash._hash_function(row[column]) == hash_value:
                results.append({col: row[col] for col in row.keys()})
        return results
    
    """
    Di bawah ini fungsi hash
    TODO: DELETE OPERATION
    UPDATE: DELETE OPERATION DONE 
    """
    
    @staticmethod
    def change_config(DATA_DIR="data_blocks/", BPLUS_DIR="bplus/"):
        BPlusReader.DATA_DIR = DATA_DIR
        BPlusReader.BPLUS_DIR = BPLUS_DIR
    
    @staticmethod
    def _get_bplus_block_file(table: str, column: str, block_id: int) -> str:
        return os.path.join(BPlusReader.DATA_DIR, BPlusReader.BPLUS_DIR, f"{table}_{column}_bplus_block_{block_id}.blk")
    
    @staticmethod
    def _load_bplus_block(table: str, column: str, block_id: int) -> List[Dict] :
        block_file = BPlusReader._get_bplus_block_file(table, column, block_id)
        if os.path.exists(block_file):
            with open(block_file, "rb") as file:
                return pickle.load(file)
        return []
    
    @staticmethod
    def _save_bplus_block(table: str, column: str, block_id: int, block_data: List[Dict]):
        block_file = BPlusReader._get_bplus_block_file(table, column, block_id)
        with open(block_file, "wb") as file:
            pickle.dump(block_data, file)
    
    @staticmethod
    def _write_bplus_block(table: str, column: str, new_block_id: int):
        blocks = sorted(int(file.split('_')[-1].split('.')[0]) 
                        for file in os.listdir(os.path.join(BPlusReader.DATA_DIR, BPlusReader.BPLUS_DIR)) 
                        if file.startswith(f"{table}_{column}_bplus"))
        block = BPlusReader._load_bplus_block(table, column, 0)
        if {'id': new_block_id} in block:
            return
        
        block.append({'id': new_block_id})
        # assumes entries fit in one block
        Hash._save_hash_block(table, column, hash_value, 0, block)
        print("HASH SAVED")
        return
    
    @staticmethod
    def _delete_hash_block(table: str, column: str, hash_value: int, old_block_id: int):
        # assumes entries fit in one block
        block = Hash._load_hash_block(table, column, hash_value, 0)
        new_block = []
        for row in block:
            if row['id'] != old_block_id:
                new_block.append(row)
        # assumes entries fit in one block
        if not new_block and hash_value != 0:
            if os.path.exists(Hash._get_hash_block_file(table, column, hash_value, old_block_id)):
                os.remove(Hash._get_hash_block_file(table, column, hash_value, old_block_id))
        else:
            Hash._save_hash_block(table, column, hash_value, 0, new_block)
        print("HASH UPDATED")
        return