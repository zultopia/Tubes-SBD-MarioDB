import ast
import inspect
import os
import pickle
import math
import sys
import textwrap
from typing import Any, List, Literal, Union, Dict, Tuple

from StorageManager.HashIndex import Hash
from ConcurrencyControlManager.utils import PrimaryKey
from FailureRecoveryManager.Buffer import Buffer

class Student:
    def __init__(self, id:int, name:Union[str, None]=None, dept_name:Union[str, None]=None, tot_cred:Union[int, None]=None):
        self.id = id
        self.name = name
        self.dept_name = dept_name
        self.tot_cred = tot_cred
        self.primary_key = PrimaryKey(id)

class Instructor:
    def __init__(self, id:int, name:Union[str, None]=None, dept_name:Union[str, None]=None, salary:Union[str, None]=None):
        self.id = id
        self.name = name
        self.dept_name = dept_name
        self.salary = salary
        self.primary_key = PrimaryKey(id)

class Department:
    def __init__(self, dept_name:str, building:Union[str, None]=None, budget:Union[int, None]=None):
        self.dept_name = dept_name
        self.building = building
        self.budget = budget
        self.primary_key = PrimaryKey(dept_name)

class Course:
    def __init__(self, course_id:int, title:Union[str, None]=None, dept_name:Union[str, None]=None, credits:Union[int, None]=None):
        self.course_id = course_id
        self.title = title
        self.dept_name = dept_name
        self.credits = credits
        self.primary_key = PrimaryKey(course_id)

class Section:
    def __init__(self, course_id:int, sec_id:str, semester:int, year:int, building:Union[str, None]=None, room_number:Union[int, None]=None, time_slot_id:Union[int, None]=None):
        self.course_id = course_id
        self.sec_id = sec_id
        self.semester = semester
        self.year = year
        self.building = building
        self.room_number = room_number
        self.time_slot_id = time_slot_id
        self.primary_key = PrimaryKey(course_id, sec_id, semester, year)

class Teaches:
    def __init__(self, id:int, course_id:int, sec_id:str, semester:int, year:int):
        self.id = id
        self.course_id = course_id
        self.sec_id = sec_id
        self.semester = semester
        self.year = year
        self.primary_key = PrimaryKey(id, course_id, sec_id, semester, year)

class Advisor:
    def __init__(self, s_id:int, i_id:Union[int, None]=None):
        self.s_id = s_id
        self.i_id = i_id
        self.primary_key = PrimaryKey(s_id)
    
class Prerequisite:
    def __init__(self, course_id:int, prereq_id:str):
        self.course_id = course_id
        self.prereq_id = prereq_id
        self.primary_key = PrimaryKey(course_id, prereq_id)

class TimeSlot:
    def __init__(self, time_slot_id:int, day:Union[str, None]=None, start_time:Union[str, None]=None, end_time:Union[str, None]=None):
        self.time_slot_id = time_slot_id
        self.day = day
        self.start_time = start_time
        self.end_time = end_time
        self.primary_key = PrimaryKey(time_slot_id)

class Takes:
    def __init__(self, id:int, course_id:int, sec_id:str, semester:int, year:int, grade:Union[str, None]=None):
        self.id = id
        self.course_id = course_id
        self.sec_id = sec_id
        self.semester = semester
        self.year = year
        self.grade = grade
        self.primary_key = PrimaryKey(id, course_id, sec_id, semester, year)

class Classroom:
    def __init__(self, building:str, room_number:int, capacity:Union[int, None]=None):
        self.building = building
        self.room_number = room_number
        self.capacity = capacity
        self.primary_key = PrimaryKey(building, room_number)

class Condition:
    def __init__(self, column: str, operation: str, operand: Union[int, str]):
        self.column = column
        self.operation = operation
        self.operand = operand
        
class ConditionGroup:
    def __init__(self, conditions: List[Union[Condition, "ConditionGroup"]], logic_operator: str = "AND"):
        self.conditions = conditions
        self.logic_operator = logic_operator.upper()  # "AND" or "OR"

class DataRetrieval:
    def __init__(self, table: str, columns: List[str], conditions: ConditionGroup, search_type: str, level: str, attribute: str = None):
        self.table = table
        self.columns = columns
        self.conditions = conditions
        self.search_type = search_type
        self.attribute = attribute # Defaultnya akan None kecuali level = "cell"

class DataWrite:
    def __init__(self, table: str, columns: List[str], new_values: List[Union[int, str]], level: str, attribute: str = None, conditions: ConditionGroup = None):
        self.table = table
        self.columns = columns
        self.new_values = new_values
        self.level = level
        self.attribute = attribute # Defaultnya akan None kecuali level = "cell"
        self.conditions = conditions or []
        self.old_new_values = []

class DataDeletion:
    def __init__(self, table: str, conditions: ConditionGroup, level: str, attribute: str = None):
        self.table = table
        self.conditions = conditions
        self.level = level
        self.attribute = attribute # Defaultnya akan None kecuali level = "cell"

class Statistic:
    def __init__(self, n_r: int, b_r: int, l_r: int, f_r: int, V_a_r: dict):
        self.n_r = n_r
        self.b_r = b_r
        self.l_r = l_r
        self.f_r = f_r
        self.V_a_r = V_a_r

class StorageManager:
    # DATA_FILE = "data.dat/"
    LOG_FILE = "log.dat"
    DATA_DIR = "data_blocks/"
    HASH_DIR = "hash/" # DATA_DIR/HASH_DIR/{table}_{column}_{hash}_{block_id}
    BLOCK_SIZE = 4096  # bytes

    def __init__(self, buffer: Union[Buffer, None]=None):
        print("INITIATING")
        os.makedirs(self.DATA_DIR, exist_ok=True)
        os.makedirs(os.path.join(self.DATA_DIR, self.HASH_DIR), exist_ok=True)
        self.buffer = buffer
        Hash.change_config(buffer=buffer)
        self.indexes = {}
        self.logs = self._load_logs()
        self.action_logs = []

    # def _load_data(self):
    #     if os.path.exists(self.DATA_FILE):
    #         with open(self.DATA_FILE, "rb") as file:
    #             return pickle.load(file)
    #     return {}

    # def _save_data(self):
    #     with open(self.DATA_FILE, "wb") as file:
    #         pickle.dump(self.data, file)
    
    def _load_logs(self):
        if os.path.exists(self.LOG_FILE):
            with open(self.LOG_FILE, "rb") as file:
                return pickle.load(file)
        return []

    def _save_logs(self):
        with open(self.LOG_FILE, "wb") as file:
            pickle.dump(self.logs, file)
            
    def _get_block_file(self, table: str, block_id: int) -> str:
        """Returns the filename of a block

        Args:
            table (str): Table name
            block_id (int): Block id

        Returns:
            str: The filename
        """
        return os.path.join(self.DATA_DIR, f"{table}__block__{block_id}.blk")

    def _load_block(self, table: str, block_id: int) -> List[Dict]:
        """Reads block from disk

        Args:
            table (str): Table name
            block_id (int): Block id

        Returns:
            List[Dict]: _description_
        """
        block_file = self._get_block_file(table, block_id)
        if os.path.exists(block_file):
            with open(block_file, "rb") as file:
                return pickle.load(file)
        return []

    def _save_block(self, table: str, block_id: int, block_data: List[Dict]) -> Any:
        """Saves block_data to disk

        Args:
            table (str): Table name
            block_id (int): Block id
            block_data (List[Dict]): The block itself
        
        Returns:
            any: The block data with id equals to block_id
        """
        block_file = self._get_block_file(table, block_id)
        if not block_data:
            if os.path.exists(block_file):
                os.remove(block_file)
                return
        with open(block_file, "wb") as file:
            pickle.dump(block_data, file)
    
    def log_action(self, action, table, data, columns=None):
        log_entry = {
            "action": action,
            "table": table,
            "data": data
        }
        if columns:
            log_entry["columns"] = columns
        self.action_logs.append(log_entry)
        
    def read_block(self, data_retrieval: DataRetrieval) -> List[Any]: 
        """Reads blocks from buffer (if exist)
        If block isn't in buffer read from disk

        Args:
            data_retrieval (DataRetrieval): Data to retrieve

        Returns:
            List[Any]: List of dictionaries satisfying data_retrieval 
        """
        table = data_retrieval.table
        columns = data_retrieval.columns
        conditions = data_retrieval.conditions
        results = []

        for file in os.listdir(self.DATA_DIR):
            if file.startswith(table):
                block_id = int(file.split('__')[-1].split('.')[0])
                block = self.buffer.get_buffer(table, block_id)
                if not block:
                    block = self._load_block(table, block_id)
                for row in block:
                    if self._evaluate_conditions(row, conditions):
                        results.append({col: row[col] for col in columns})
        return results
        
    def write_block_to_disk(self, table: str, block_id: int, block_data: Dict) -> int:
        """Writes blocks straight to disk. Automatically syncs index

        Args:
            data_write (DataWrite): Data to write

        Returns:
            int: Number of rows affected
        """
        self._save_block(table, block_id, block_data)
        # table = data_write.table
        # columns = data_write.columns
        # new_values = data_write.new_values
        # conditions = data_write.conditions
        # dict_new_values = dict(zip(columns, new_values))
        # for column_exist in self.get_all_attributes(table):
        #     if column_exist not in dict_new_values.keys():
        #         dict_new_values[column_exist] = None
        # blocks = sorted(int(file.split('__')[-1].split('.')[0]) for file in os.listdir(self.DATA_DIR) if file.startswith(table))
        # if not data_write.conditions:
        #     # add operation
        #     for block_id in blocks:
        #         block = self.buffer.get_buffer(table, block_id) 
        #         if not block:
        #             block = self._load_block(table, block_id)
        #         if len(block) < self.BLOCK_SIZE:
        #             block.append(dict_new_values)
        #             self._save_block(table, block_id, block)
        #             self.update_all_column_with_hash_to_disk(table, columns, dict_new_values, block_id)
        #             return 1
        #     # If no space, create a new block
        #     new_block_id = max(blocks, default=-1) + 1
        #     new_block = [dict_new_values]
        #     self._save_block(table, new_block_id, new_block)
        #     self.update_all_column_with_hash_to_disk(table, columns, dict_new_values, new_block_id)
        #     # self.update_all_column_with_hash(table, columns, dict_new_values, new_block_id)
        #     self.log_action("write", table, {"block_id": new_block_id, "data": new_values})
        #     return 1
        # # update operation 
        # num_updated = 0
        # for block_id in blocks:
        #     block = self.buffer.get_buffer(table, block_id)
        #     if not block:
        #         block = self._load_block(table, block_id)
        #     new_block = []
        #     data_changed = False
        #     for row in block:
        #         if self._evaluate_conditions(row, conditions):
        #             data_changed = True
        #             new_row = row
        #             for column, new_value in dict_new_values.items():
        #                 # self.delete_all_column_with_hash_to_disk(table, [column], {column: row[column]}, block_id)
        #                 new_row[column] = new_value
        #                 # self.update_all_column_with_hash_to_disk(table, [column], {column: new_row[column]}, block_id)
        #             new_block.append(new_row)
        #             num_updated += 1
        #         else:
        #             new_block.append(row)
        #     if data_changed:
        #         self._save_block(table, block_id, new_block)
        # return num_updated

    def write_block(self, data_write: DataWrite) -> int:
        """Writes to buffer. Automatically syncs index

        Args:
            data_write (DataWrite): Data to write

        Returns:
            int: The number of rows affected
            (for INSERT operation this is always 1) 
        """
        table = data_write.table
        columns = data_write.columns
        new_values = data_write.new_values
        conditions = data_write.conditions
        dict_new_values = dict(zip(columns, new_values))
        for column_exist in self.get_all_attributes(table):
            if column_exist not in dict_new_values.keys():
                dict_new_values[column_exist] = None
        blocks = sorted(int(file.split('__')[-1].split('.')[0]) for file in os.listdir(self.DATA_DIR) if file.startswith(table))
        if not data_write.conditions:
            # add operation
            for block_id in blocks:
                block = self.buffer.get_buffer(table, block_id) 
                if not block:
                    block = self._load_block(table, block_id)
                if len(block) < self.BLOCK_SIZE:
                    block.append(dict_new_values)
                    self.buffer.put_buffer(table, block_id, block)
                    self.update_all_column_with_hash(table, columns, dict_new_values, block_id)
                    return 1
            # If no space, create a new block
            new_block_id = max(blocks, default=-1) + 1
            new_block = [dict_new_values]
            
            self.buffer.put_buffer(table, new_block_id, new_block)
            self.update_all_column_with_hash(table, columns, dict_new_values, new_block_id)
            # give clue it exists
            self._save_block(table, new_block_id, [])
            return 1
        # update operation 
        num_updated = 0
        for block_id in blocks:
            block = self.buffer.get_buffer(table, block_id)
            if not block:
                block = self._load_block(table, block_id)
            new_block = []
            data_changed = False
            for row in block:
                if self._evaluate_conditions(row, conditions):
                    data_changed = True
                    new_row = row
                    for column, new_value in dict_new_values.items():
                        self.delete_all_column_with_hash(table, [column], {column: row[column]}, block_id)
                        new_row[column] = new_value
                        self.update_all_column_with_hash(table, [column], {column: new_row[column]}, block_id)
                    new_block.append(new_row)
                    num_updated += 1
                else:
                    new_block.append(row)
            if data_changed:
                self.buffer.put_buffer(table, block_id, new_block)
        return num_updated
    
    def delete_block_to_disk(self, table: str, block_id: int) -> int:
        """Deletes block in disk. Automatically syncs index

        Args:
            data_deletion (DataDeletion): Data to delete

        Returns:
            int: Number of rows affected
        """
        self._save_block(table, block_id, None)
        # table = data_deletion.table
        # conditions = data_deletion.conditions
        # total_deleted = 0

        # for file in os.listdir(self.DATA_DIR):
        #     if file.startswith(table):
        #         block_id = int(file.split('__')[-1].split('.')[0])
        #         block = self.buffer.get_buffer(table, block_id)
        #         if not block:
        #             block = self._load_block(table, block_id)
        #         new_block = [row for row in block if not self._evaluate_conditions(row, conditions)]
        #         total_deleted += len(block) - len(new_block)
        #         deleted_block = [row for row in block if row not in new_block]
        #         print("DELETED BLOCK", deleted_block)
        #         # for row in deleted_block:
        #         #     self.delete_all_column_with_hash_to_disk(table, self.get_all_attributes(table), row, block_id)
        #         if not new_block:
        #             new_block = None
        #         self._save_block(table, block_id, new_block)
        #         # self.delete_all_column_with_hash(table, self.get_all_attributes(table), block, block_id)
        # return total_deleted
    
    def delete_block(self, data_deletion: DataDeletion) -> int:
        """Deletes blocks, putting blocks in buffer. Automatically syncs index

        Args:
            data_deletion (DataDeletion): Data to delete

        Returns:
            int: Total rows deleted
        """
        table = data_deletion.table
        conditions = data_deletion.conditions
        total_deleted = 0

        for file in os.listdir(self.DATA_DIR):
            if file.startswith(table):
                block_id = int(file.split('__')[-1].split('.')[0])
                block = self.buffer.get_buffer(table, block_id)
                if not block:
                    block = self._load_block(table, block_id)
                new_block = [row for row in block if not self._evaluate_conditions(row, conditions)]
                total_deleted += len(block) - len(new_block)
                deleted_block = [row for row in block if row not in new_block]
                for row in deleted_block:
                    self.delete_all_column_with_hash(table, self.get_all_attributes(table), row, block_id)
                if not new_block:
                    new_block = None
                self.buffer.put_buffer(table, block_id, new_block)
        return total_deleted
    
    def write_hash_block_to_disk(self, table: str, column: str, hash_value: int, block_id: int, block_data: Dict):
        Hash._save_hash_block_to_disk(table, column, hash_value, block_id, block_data)
        
    def delete_hash_block_to_disk(self, table: str, column: str, hash_value: str, block_id: int):
        Hash._save_hash_block_to_disk(table, column, hash_value, block_id, None)
    
    def get_stats(self) -> Dict[str, Statistic]:
        """Get summary of the whole schema

        Returns:
            Dict[str, Statistic]: Statistic of every table in the schema
        """
        stats = {}
        for file in os.listdir(self.DATA_DIR):
            if file.endswith(".blk"):
                parts = file.split("__block__")
                table_name = parts[0]
                block_id = int(parts[1].split(".")[0])
                block = self.buffer.get_buffer(table_name, block_id)
                if not block: 
                    block = self._load_block(table_name, block_id)
                if table_name not in stats:
                    stats[table_name] = {
                        "n_r": 0, 
                        "b_r": 0,  
                        "l_r": 0,  
                        "f_r": 0,  
                        "V_a_r": {} 
                    }
                stats[table_name]["n_r"] += len(block)
                stats[table_name]["b_r"] += 1
                if block:
                    print(block)
                    for row in block:
                        print(row)
                        for col in row.keys():
                            if col not in stats[table_name]["V_a_r"]:
                                stats[table_name]["V_a_r"][col] = set()
                            stats[table_name]["V_a_r"][col].update(row[col] for row in block)
                    # row_length = len(block[0])
        for table_name, table_stats in stats.items():
            if table_stats["l_r"] > 0:
                table_stats["f_r"] = math.floor(self.BLOCK_SIZE / table_stats["l_r"])
            table_stats["V_a_r"] = {col: len(values) for col, values in table_stats["V_a_r"].items()}
            stats[table_name] = Statistic(
                n_r=table_stats["n_r"],
                b_r=table_stats["b_r"],
                l_r=table_stats["l_r"],
                f_r=table_stats["f_r"],
                V_a_r=table_stats["V_a_r"]
            )
        return stats
    
    def get_index(self, relation: str, attribute: str) -> Union[Literal["hash", "btree"], None]:
        """Get the type of index on the given attribute in the relation."""
        stuff = 0
        for file in os.listdir(os.path.join(self.DATA_DIR, self.HASH_DIR)):
            print(file)
            stuff += 1
            if file.startswith(f"{relation}__{attribute}__hash"):
                return "hash"
        print(stuff)
        return None
    
    def has_index(self, attribute: str, relation: str) -> bool:
        """Check if the attribute in the relation has an index."""
        for file in os.listdir(os.path.join(self.DATA_DIR, self.HASH_DIR)):
            if file.startswith(f"{relation}__{attribute}__hash"):
                return True
        return False
    
    def get_all_relations(self) -> List[str]:
        """Get all relations in the data."""
        relations = []
        helper_classes = ['Condition', 'ConditionGroup', 'DataDeletion', 'DataRetrieval', 'DataWrite', 'Statistic']
        for name, obj in inspect.getmembers(sys.modules[__name__], lambda member: inspect.isclass(member) and member.__module__ == __name__):
            if name != type(self).__name__ and name not in helper_classes:
                relations.append(name)
        return relations

    def get_all_attributes(self, relation: str) -> List[str]:
        """Get all attributes in the relation."""
        cls = globals()[relation]
        init_method = getattr(cls, "__init__", None)
        if not init_method:
            return []

        source = textwrap.dedent(inspect.getsource(init_method))
        tree = ast.parse(source)
        attributes = [node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute) and isinstance(node.ctx, ast.Store) and node.attr != "primary_key"]
        return attributes
    
    def has_relation(self, relation: str) -> bool:
        """Check if the relation is in the data."""
        return relation in self.get_all_relations()
    
    def has_attribute(self, attribute: str, relation: str) -> bool:
        """Check if the attribute is in the relation."""
        return attribute in self.get_all_attributes(relation)

    def set_index(self, table: str, column: str, index_type: str) -> None:
        if index_type != "hash" and index_type != "B+":
            raise ValueError("Index yang digunakan adalah hash index.")
        
        if index_type == "hash":
            exist = False
            for file in os.listdir(os.path.join(self.DATA_DIR, self.HASH_DIR)):
                if file.startswith(f"{table}__{column}__hash"):
                    exist = True
                    break
            if exist:
                print(f"Hash index already exists at {table}.{column}")
                return
            Hash._initiate_block(table, column)
            for file in os.listdir(self.DATA_DIR):
                if file.startswith(f"{table}"):
                    block_id = int(file.split('__')[-1].split('.')[0])
                    block = self.buffer.get_buffer(table, block_id)
                    if not block:
                        block = self._load_block(table, block_id)
                    print("IN SET INDEX", block)
                    for row in block:
                        self.write_block_with_hash(table, column, row[column], block_id)
            print(f"Hash index dibuat pada {table}.{column}")
        else:
            raise NotImplementedError("B+ Not Implemented")
        
    def read_block_with_hash(self, table: str, column: str, value) -> List[Dict]:
        """Get every row in table.column with column equals value

        Args:
            table (str): Table name
            column (str): Column name
            value (_type_): Value to search

        Returns:
            List[Dict]: All rows with column = value
        """
        return Hash._get_rows(table, column, value)
    
    def delete_all_column_with_hash(self, table: str, changed_columns: List[str], old_values: Dict, old_block_id: int) -> None:
        """Do NOT call from outside this module. Helper function to delete hash 

        Args:
            table (str): Table name
            changed_columns (List[str]): Changed columns
            old_values (Dict): Old values
            old_block_id (int): Old block id to remove from hash
        """
        for column in changed_columns:
            hash_exist = False
            for file in os.listdir(os.path.join(self.DATA_DIR, self.HASH_DIR)):
                if file.startswith(f"{table}__{column}__hash"):
                    hash_exist = True
                    break
            if not hash_exist:
                continue
            Hash._delete_row(table, column, old_block_id, old_values[column])
    
    def update_all_column_with_hash(self, table: str, changed_columns: List[str], new_values: Dict, new_block_id: int) -> None:
        """Do NOT call from outside this module. Helper function to update hash

        Args:
            table (str): Table name
            changed_columns (List[str]): Changed columns
            new_values (Dict): New values
            new_block_id (int): New block id to add
        """
        for column in changed_columns:
            hash_exist = False
            for file in os.listdir(os.path.join(self.DATA_DIR, self.HASH_DIR)):
                if file.startswith(f"{table}__{column}__hash"):
                    hash_exist = True
                    break
            if not hash_exist:
                continue
            Hash._write_row(table, column, new_block_id, new_values[column])
    
    def delete_all_column_with_hash_to_disk(self, table: str, changed_columns: List[str], old_values: Dict, old_block_id: int) -> None:
        """Do NOT call from outside this module. 
        Helper function to delete hash directly from disk

        Args:
            table (str): Table name
            changed_columns (List[str]): Changed columns
            old_values (Dict): Old values
            old_block_id (int): Old block id to delete 
        """
        for column in changed_columns:
            hash_exist = False
            for file in os.listdir(os.path.join(self.DATA_DIR, self.HASH_DIR)):
                if file.startswith(f"{table}__{column}__hash"):
                    hash_exist = True
                    break
            if not hash_exist:
                continue
            Hash._delete_row_to_disk(table, column, old_block_id, old_values[column])
    
    def update_all_column_with_hash_to_disk(self, table: str, changed_columns: List[str], new_values: Dict, new_block_id: int) -> None:
        """Do NOT call from outside this module.
        Helper function to update hash directly from disk.

        Args:
            table (str): Table name
            changed_columns (List[str]): Changed columns
            new_values (Dict): New values
            new_block_id (int): New block id to add
        """
        for column in changed_columns:
            hash_exist = False
            for file in os.listdir(os.path.join(self.DATA_DIR, self.HASH_DIR)):
                if file.startswith(f"{table}__{column}__hash"):
                    hash_exist = True
                    break
            if not hash_exist:
                continue
            Hash._write_row_to_disk(table, column, new_block_id, new_values[column])
                   
    def read_block_with_hash(self, table: str, column: str, value) -> List[Dict]:
        """Get every row from table.column with column = value

        Args:
            table (str): Table name
            column (str): Column name
            value (_type_): Value to search

        Returns:
            List[Dict]: Every row that have column = value
        """
        return Hash._get_rows(table, column, value)
    
    def write_block_with_hash(self, table: str, column: str, value, new_block_id: int) -> None:
        """Do NOT call from outside this module.
        Helper function to update hash 

        Args:
            table (str): Table name
            column (str): Column name
            value (_type_): Value to update
            new_block_id (int): New block id to add
        """
        Hash._write_row(table, column, new_block_id, value)
        
    def _evaluate_condition(self, row: Dict, condition: Condition):
        if condition.column not in row.keys() or row[condition.column] is None:
            return False
        value = row[condition.column]
        operand = condition.operand
        operation = condition.operation
        return {
            "=": value == operand,
            "<>": value != operand,
            ">": value > operand,
            ">=": value >= operand,
            "<": value < operand,
            "<=": value <= operand,
        }[operation]

    def _evaluate_conditions(self, row: Dict, condition_group: ConditionGroup):
        if condition_group.logic_operator == "AND":
            return all(
                self._evaluate_conditions(row, cond) if isinstance(cond, ConditionGroup) 
                else self._evaluate_condition(row, cond)
                for cond in condition_group.conditions
            )
        elif condition_group.logic_operator == "OR":
            return any(
                self._evaluate_conditions(row, cond) if isinstance(cond, ConditionGroup) 
                else self._evaluate_condition(row, cond)
                for cond in condition_group.conditions
            )
        else:
            raise ValueError("Invalid logic_operator. Use 'AND' or 'OR'.")
