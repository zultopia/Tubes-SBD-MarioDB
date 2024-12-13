import os
import pickle
from typing import Any, Dict, List, Union

from FailureRecoveryManager.Buffer import Buffer

class Hash(object):
    MAX_BUCKET = 10000
    DATA_DIR = "data_blocks/"
    HASH_DIR = "hash/"
    buffer = None
    
    @staticmethod
    def _get_block_file(table: str, block_id: int) -> str:
        return os.path.join(Hash.DATA_DIR, f"{table}__block__{block_id}.blk")
    
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
        block = Hash.buffer.get_buffer(table, block_id)
        if not block:
            block = Hash._load_block(table, block_id)
        for row in block:
            if Hash._hash_function(row[column]) == hash_value:
                results.append({col: row[col] for col in row.keys()})
        return results
    
    @staticmethod
    def change_config(DATA_DIR="data_blocks/", HASH_DIR="hash/", buffer: Union[Buffer, None]=None):
        Hash.DATA_DIR = DATA_DIR
        Hash.buffer = buffer
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
    def _get_hash_buffer_block_file(table: str, column: str, hash_value: str) -> str:
        return f"hash:{hash_value}:{table}:{column}"
    
    @staticmethod
    def _get_hash_block_file(table: str, column: str, hash_value: int, block_id: int) -> str:
        return os.path.join(Hash.DATA_DIR, Hash.HASH_DIR, f"{table}__{column}__hash__{hash_value}__block__{block_id}.blk")
    
    @staticmethod
    def _load_hash_block(table: str, column: str, hash_value: int, block_id: int) -> List[Dict] :
        block_file = Hash._get_hash_block_file(table, column, hash_value, block_id)
        if os.path.exists(block_file):
            with open(block_file, "rb") as file:
                return pickle.load(file)
        return []
    
    @staticmethod
    def _save_hash_block(table: str, column: str, hash_value: int, block_id: int, block_data: List[Dict]):
        # block_file = Hash._get_hash_buffer_block_file(table, column, hash_value)
        Hash.buffer.put_buffer_hash(hash_value, table, block_id, column, block_data)
        # with open(block_file, "wb") as file:
        #    pickle.dump(block_data, file)
        
    @staticmethod
    def _save_hash_block_to_disk(table: str, column: str, hash_value: int, block_id: int, block_data: List[Dict]):
        block_file = Hash._get_hash_block_file(table, column, hash_value, block_id)
        if block_data is None:
            if os.path.exists(Hash._get_hash_block_file(table, column, hash_value, block_id)):
               os.remove(Hash._get_hash_block_file(table, column, hash_value, block_id))
            return
        with open(block_file, "wb") as file:
           pickle.dump(block_data, file)
    
    @staticmethod
    def _write_hash_block(table: str, column: str, hash_value: int, new_block_id: int):
        # assumes entries fit in one block
        block = Hash.buffer.get_buffer_hash(hash_value, table, 0, column)
        if not block:
            block = Hash._load_hash_block(table, column, hash_value, 0)
        block.append({'id': new_block_id})
        # assumes entries fit in one block
        # Hash.buffer.put_buffer(Hash._get_hash_buffer_block_file(table, column, hash_value), 0, block)
        Hash._save_hash_block(table, column, hash_value, 0, block)
        # print("HASH SAVED IN BUFFER")
        return
    
    @staticmethod
    def _write_hash_block_to_disk(table: str, column: str, hash_value: str, new_block_id: int):
        # assumes entries fit in one block
        block = Hash.buffer.get_buffer_hash(hash_value, table, 0, column)
        if not block:
            block = Hash._load_hash_block(table, column, hash_value, 0)
        block.append({'id': new_block_id})
        # assumes entries fit in one block
        Hash._save_hash_block_to_disk(table, column, hash_value, 0, block)
        # print("HASH SAVED IN DISK")
        return
    
    @staticmethod
    def _delete_hash_block(table: str, column: str, hash_value: int, old_block_id: int):
        # assumes entries fit in one block
        block = Hash.buffer.get_buffer_hash(hash_value, table, 0, column)
        if not block:
            Hash._load_hash_block(table, column, hash_value, 0)
        new_block = []
        found = False
        for row in block:
            if row['id'] != old_block_id or found:
                new_block.append(row)
            else:
                found = True
        # assumes entries fit in one block
        if not new_block and hash_value != 0:
            Hash.buffer.put_buffer_hash(hash_value, table, 0, column, None)
            # if os.path.exists(Hash._get_hash_block_file(table, column, hash_value, old_block_id)):
            #    os.remove(Hash._get_hash_block_file(table, column, hash_value, old_block_id))
        else:
            Hash.buffer.put_buffer_hash(hash_value, table, 0, column, new_block)
            # Hash._save_hash_block(table, column, hash_value, 0, new_block)
        # print("HASH UPDATED")
        return
    
    @staticmethod
    def _delete_hash_block_to_disk(table: str, column: str, hash_value: int, old_block_id: int):
        # assumes entries fit in one block
        block = Hash.buffer.get_buffer_hash(hash_value, table, 0, column)
        if not block:
            block = Hash._load_hash_block(table, column, hash_value, 0)
        new_block = []
        found = False
        for row in block:
            if row['id'] != old_block_id or found:
                new_block.append(row)
            else:
                found = True
        # assumes entries fit in one block
        if not new_block and hash_value != 0:
            if os.path.exists(Hash._get_hash_block_file(table, column, hash_value, old_block_id)):
               os.remove(Hash._get_hash_block_file(table, column, hash_value, old_block_id))
        else:
            Hash._save_hash_block_to_disk(table, column, hash_value, 0, new_block)
        # print("HASH UPDATED")
        return

    @staticmethod
    def _get_rows(table: str, column: str, value):
        results = []
        block_id = 0
        hash_value = Hash._hash_function(value)
        id_read = []
        # assumes entries fit in one block
        while(os.path.exists(Hash._get_hash_block_file(table, column, hash_value, block_id)) or 
              Hash.buffer.get_buffer_hash(hash_value, table, block_id, column) is not None):
            block = Hash.buffer.get_buffer_hash(hash_value, table, block_id, column)
            if not block:
                block = Hash._load_hash_block(table, column, hash_value, block_id)
            for row in block:
                if row['id'] not in id_read:
                    results.extend(Hash.read_block(table, column, hash_value, row['id']))
                    id_read.append(row['id'])
            break # hapus ketika asumsi di atas salah
        return results
    
    @staticmethod
    def _write_row(table: str, column: str, new_block_id: int, value):
        hash_value = Hash._hash_function(value)
        # print("HASH", hash_value)
        Hash._write_hash_block(table, column, hash_value, new_block_id)
            
    @staticmethod
    def _delete_row(table: str, column: str, old_block_id: int, value):
        hash_value = Hash._hash_function(value)
        # print("HASH", hash_value)
        Hash._delete_hash_block(table, column, hash_value, old_block_id)
        
    @staticmethod
    def _write_row_to_disk(table: str, column: str, new_block_id: int, value):
        hash_value = Hash._hash_function(value)
        # print("HASH", hash_value)
        Hash._write_hash_block_to_disk(table, column, hash_value, new_block_id)
            
    @staticmethod
    def _delete_row_to_disk(table: str, column: str, old_block_id: int, value):
        hash_value = Hash._hash_function(value)
        # print("HASH", hash_value)
        Hash._delete_hash_block_to_disk(table, column, hash_value, old_block_id)

    @staticmethod
    def _initiate_block(table: str, column: str):
        Hash._save_hash_block_to_disk(table, column, 0, 0, [])
        
    
    
    
