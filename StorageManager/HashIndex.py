import os
import pickle
from typing import Any, Dict, List

class Hash(object):
    MAX_BUCKET = 10000
    DATA_DIR = "data_blocks/"
    HASH_DIR = "hash/"
    
    """
    Bawah ini copasan StorageManager
    """
    @staticmethod
    def _get_block_file(table: str, block_id: int) -> str:
        return os.path.join(Hash.DATA_DIR, f"{table}_block_{block_id}.blk")
    
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
    """
    
    @staticmethod
    def change_config(DATA_DIR="data_blocks/", HASH_DIR="hash/"):
        Hash.DATA_DIR = DATA_DIR
        Hash.HASH_DIR = HASH_DIR
        
    @staticmethod
    # have to be stable (unchanged) 
    def _hash_function(obj: Any) -> int:
        obj_str = str(obj)
        
        def encode_str(s):
            hash_value = 0
            for char in s:
                hash_value = (hash_value * 31 + ord(char)) % (2**32)
            return hash_value

        def lcg(seed, a=1664525, c=1013904223, m=2**32):
            return (a * seed + c) % m
        
        seed = encode_str(obj_str)
        random_value = lcg(seed)
        
        return random_value % (Hash.MAX_BUCKET)
    
    @staticmethod
    def _get_hash_block_file(table: str, column: str, hash_value: int, block_id: int) -> str:
        return os.path.join(Hash.DATA_DIR, Hash.HASH_DIR, f"{table}_{column}_hash_{hash_value}_block_{block_id}.blk")
    
    @staticmethod
    def _load_hash_block(table: str, column: str, hash_value: int, block_id: int) -> List[Dict] :
        block_file = Hash._get_hash_block_file(table, column, hash_value, block_id)
        if os.path.exists(block_file):
            with open(block_file, "rb") as file:
                return pickle.load(file)
        return []
    
    @staticmethod
    def _save_hash_block(table: str, column: str, hash_value: int, block_id: int, block_data: List[Dict]):
        block_file = Hash._get_hash_block_file(table, column, hash_value, block_id)
        with open(block_file, "wb") as file:
            pickle.dump(block_data, file)
    
    @staticmethod
    def _write_hash_block(table: str, column: str, hash_value: int, new_block_id: int):
        # assumes entries fit in one block
        block = Hash._load_hash_block(table, column, hash_value, 0)
        if {'id': new_block_id} in block:
            return
        
        block.append({'id': new_block_id})
        # assumes entries fit in one block
        Hash._save_hash_block(table, column, hash_value, 0, block)
        print("HASH SAVED")
        return

    @staticmethod
    def _get_rows(table: str, column: str, value):
        results = []
        block_id = 0
        hash_value = Hash._hash_function(value)
        # assumes entries fit in one block
        while(os.path.exists(Hash._get_hash_block_file(table, column, hash_value, block_id))):
            block = Hash._load_hash_block(table, column, hash_value, block_id)
            for row in block:
                results.extend(Hash.read_block(table, column, hash_value, row['id']))
            break # hapus ketika asumsi di atas salah
        return results
    
    @staticmethod
    def _write_row(table: str, column: str, new_block_id: int, value):
        hash_value = Hash._hash_function(value)
        print("HASH", hash_value)
        Hash._write_hash_block(table, column, hash_value, new_block_id)

    
    
    
