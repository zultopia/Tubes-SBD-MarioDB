import os
import pickle
import math
from typing import List, Union, Dict, Tuple

from StorageManager.HashIndex import Hash
from .BPlusTree import BPlusTree
from ConcurrencyControlManager.classes import PrimaryKey
from FailureRecoveryManager.FailureRecoveryManager import FailureRecoveryManager

class Student:
    def __init__(self, id:int, name:str, dept_name:str, tot_cred:int):
        self.id = id
        self.name = name
        self.dept_name = dept_name
        self.tot_cred = tot_cred
        self.primary_key = PrimaryKey(id)

class Instructor:
    def __init__(self, id:int, name:str, dept_name:str, salary:int):
        self.id = id
        self.name = name
        self.dept_name = dept_name
        self.salary = salary
        self.primary_key = PrimaryKey(id)

class Department:
    def __init__(self, dept_name:str, building:str, budget:int):
        self.dept_name = dept_name
        self.building = building
        self.budget = budget
        self.primary_key = PrimaryKey(dept_name)

class Course:
    def __init__(self, course_id:int, title:str, dept_name:str, credits:int):
        self.course_id = course_id
        self.title = title
        self.dept_name = dept_name
        self.credits = credits
        self.primary_key = PrimaryKey(course_id)

class Section:
    def __init__(self, course_id:int, sec_id:str, semester:int, year:int, building:str, room_number:int, time_slot_id:int):
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
    def __init__(self, s_id:int, i_id:int):
        self.s_id = s_id
        self.i_id = i_id
        self.primary_key = PrimaryKey(s_id)
    
class Prerequisite:
    def __init__(self, course_id:int, prereq_id:str):
        self.course_id = course_id
        self.prereq_id = prereq_id
        self.primary_key = PrimaryKey(course_id, prereq_id)

class TimeSlot:
    def __init__(self, time_slot_id:int, day:str, start_time:str, end_time:str):
        self.time_slot_id = time_slot_id
        self.day = day
        self.start_time = start_time
        self.end_time = end_time
        self.primary_key = PrimaryKey(time_slot_id)

class Takes:
    def __init__(self, id:int, course_id:int, sec_id:str, semester:int, year:int, grade:str):
        self.id = id
        self.course_id = course_id
        self.sec_id = sec_id
        self.semester = semester
        self.year = year
        self.grade = grade
        self.primary_key = PrimaryKey(id, course_id, sec_id, semester, year)

class Classroom:
    def __init__(self, building:str, room_number:int, capacity:int):
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
    DATA_FILE = "data.dat/"
    LOG_FILE = "log.dat"
    DATA_DIR = "data_blocks/"
    HASH_DIR = "hash/" # DATA_DIR/HASH_DIR/{table}_{column}_{hash}_{block_id}
    BLOCK_SIZE = 4096  # bytes

    def __init__(self, frm: FailureRecoveryManager):
        print("INITIATING")
        os.makedirs(self.DATA_DIR, exist_ok=True)
        os.makedirs(os.path.join(self.DATA_DIR, self.HASH_DIR), exist_ok=True)
        self.frm = frm
        self.indexes = {}
        self.bplusindexes: Dict[object, Tuple[str, BPlusTree]] = {}
        self.logs = self._load_logs()
        self.action_logs = []

    def _load_data(self):
        if os.path.exists(self.DATA_FILE):
            with open(self.DATA_FILE, "rb") as file:
                return pickle.load(file)
        return {}

    def _save_data(self):
        with open(self.DATA_FILE, "wb") as file:
            pickle.dump(self.data, file)
    
    def _load_logs(self):
        if os.path.exists(self.LOG_FILE):
            with open(self.LOG_FILE, "rb") as file:
                return pickle.load(file)
        return []

    def _save_logs(self):
        with open(self.LOG_FILE, "wb") as file:
            pickle.dump(self.logs, file)
            
    def _get_block_file(self, table: str, block_id: int) -> str:
        return os.path.join(self.DATA_DIR, f"{table}_block_{block_id}.blk")

    def _load_block(self, table: str, block_id: int) -> List[Dict]:
        block_file = self._get_block_file(table, block_id)
        if os.path.exists(block_file):
            with open(block_file, "rb") as file:
                return pickle.load(file)
        return []

    def _save_block(self, table: str, block_id: int, block_data: List[Dict]):
        block_file = self._get_block_file(table, block_id)
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

    def write_block(self, data_write: DataWrite):
        table = data_write.table
        columns = data_write.columns
        new_values = data_write.new_values
        conditions = data_write.conditions
        dict_new_values = dict(zip(columns, new_values))
        blocks = sorted(int(file.split('_')[-1].split('.')[0]) for file in os.listdir(self.DATA_DIR) if file.startswith(table))
        if not data_write.conditions:
            # add operation
            for block_id in blocks:
                block = self.frm.get_buffer(table, block_id)
                if not block:
                    block = self._load_block(table, block_id)
                if len(block) < self.BLOCK_SIZE:
                    block.append(dict_new_values)
                    self._save_block(table, block_id, block)
                    self.frm.put_buffer(table, block_id, block)
                    self.update_all_column_with_hash(table, columns, dict_new_values, block_id)
                    self.log_action("write", table, {"block_id": block_id, "data": new_values})
                    return 1

            # If no space, create a new block
            new_block_id = max(blocks, default=-1) + 1
            new_block = [dict_new_values]
            self._save_block(table, new_block_id, new_block)
            self.frm.put_buffer(table, new_block_id, new_block)
            self.update_all_column_with_hash(table, columns, dict_new_values, new_block_id)
            self.log_action("write", table, {"block_id": new_block_id, "data": new_values})
            return 1
        # UPDATE 
        num_updated = 0
        for block_id in blocks:
            block = self.frm.get_buffer(table, block_id)
            if not block:
                block = self._load_block(table, block_id)
            new_block = []
            for row in block:
                if self._evaluate_conditions(row, conditions):
                    new_row = row
                    for column, new_value in dict_new_values.items():
                        self.delete_all_column_with_hash(table, [column], {column: row[column]}, block_id)
                        new_row[column] = new_value
                        self.update_all_column_with_hash(table, [column], {column: new_row[column]}, block_id)
                    new_block.append(new_row)
                    num_updated += 1
                else:
                    new_block.append(row)
            self._save_block(table, block_id, new_block)
            self.frm.put_buffer(table, block_id, new_block)

        return num_updated
    
    def read_block(self, data_retrieval: DataRetrieval):
        table = data_retrieval.table
        columns = data_retrieval.columns
        conditions = data_retrieval.conditions
        results = []

        for file in os.listdir(self.DATA_DIR):
            if file.startswith(table):
                block_id = int(file.split('_')[-1].split('.')[0])
                block = self.frm.get_buffer(table, block_id)
                if not block:
                    block = self._load_block(table, block_id)
                for row in block:
                    if self._evaluate_conditions(row, conditions):
                        results.append({col: row[col] for col in columns})
        return results
    
    """
    TODO: DELETE FROM HASH
    """
    def delete_block(self, data_deletion: DataDeletion):
        table = data_deletion.table
        conditions = data_deletion.conditions
        total_deleted = 0

        for file in os.listdir(self.DATA_DIR):
            if file.startswith(table):
                block_id = int(file.split('_')[-1].split('.')[0])
                block = self.frm.get_buffer(table, block_id)
                if not block:
                    block = self._load_block(table, block_id)
                new_block = [row for row in block if not self._evaluate_conditions(row, conditions)]
                total_deleted += len(block) - len(new_block)
                self._save_block(table, block_id, new_block)
                self.frm.put_buffer(table, block_id, new_block)
        return total_deleted

    def set_index(self, table: str, column: str, index_type: str):
        if index_type != "hash" and index_type != "B+":
            raise ValueError("Index yang digunakan adalah hash index.")
        
        if index_type == "hash":
            exist = False
            for file in os.listdir(os.path.join(self.DATA_DIR, self.HASH_DIR)):
                if file.startswith(f"{table}_{column}_hash"):
                    exist = True
                    break
            if exist:
                print(f"Hash index already exists at {table}.{column}")
                return
            Hash._initiate_block(table, column)
            for file in os.listdir(self.DATA_DIR):
                if file.startswith(f"{table}"):
                    block_id = int(file.split('_')[-1].split('.')[0])
                    block = self._load_block(table, block_id)
                    for row in block:
                        self.write_block_with_hash(table, column, row[column], block_id)
            print(f"Hash index dibuat pada {table}.{column}")
        else:
            raise NotImplementedError("B+ Not Implemented")
            """
            if table in self.bplusindexes.keys:
                raise ValueError(f"Table {table} sudah memiliki index B+ Tree di kolom {self.bplusindexes[table][0]}.")
            self.bplusindexes[table] = (column, BPlusTree(8))
            for row in self.data[table]:
                key = row[column]
                self.bplusindexes[table][1].insert(key, row)
            """
    
    def delete_all_column_with_hash(self, table: str, changed_columns: List[str], old_values: Dict, old_block_id: int):
        for column in changed_columns:
            hash_exist = False
            for file in os.listdir(os.path.join(self.DATA_DIR, self.HASH_DIR)):
                if file.startswith(f"{table}_{column}_hash"):
                    hash_exist = True
                    break
            if not hash_exist:
                continue
            Hash._delete_row(table, column, old_block_id, old_values[column])
    
    def update_all_column_with_hash(self, table: str, changed_columns: List[str], new_values: Dict, new_block_id: int):
        for column in changed_columns:
            hash_exist = False
            for file in os.listdir(os.path.join(self.DATA_DIR, self.HASH_DIR)):
                if file.startswith(f"{table}_{column}_hash"):
                    hash_exist = True
                    break
            if not hash_exist:
                continue
            Hash._write_row(table, column, new_block_id, new_values[column])
                   
    
    def read_block_with_hash(self, table: str, column: str, value):
        # Gunain indeks kalo ada
        return Hash._get_rows(table, column, value)
    
    
    def write_block_with_hash(self, table: str, column: str, value, new_block_id: int):
        Hash._write_row(table, column, new_block_id, value)
        
    """
    def read_range_with_bplus(self, table: str, column: str, min, max):
        index_key = table
        if index_key not in self.bplusindexes.keys or self.bplusindexes[index_key][0] != column:
            raise ValueError(f"Indeks B+ tidak ditemukan untuk {table}.{column}.")
        return self.bplusindexes[index_key][1].range_query(min, max)
    
    def read_one_with_bplus(self, table: str, column: str, value):
        index_key = table
        if index_key not in self.bplusindexes.keys or self.bplusindexes[index_key][0] != column:
            raise ValueError(f"Indeks B+ tidak ditemukan untuk {table}.{column}.")
        query_result = self.bplusindexes[index_key][1].query(value)
        if query_result is None:
            raise ValueError(f"Key {value} tidak ditemukan di {table}.{column}")
        return query_result
    """
        
    def get_stats(self):
        stats = {}
        for file in os.listdir(self.DATA_DIR):
            if file.endswith(".blk"):
                parts = file.split("_block_")
                table_name = parts[0]
                block_id = int(parts[1].split(".")[0])
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
                    row_length = len(block[0])
                    stats[table_name]["l_r"] = row_length
                    for col in block[0].keys():
                        if col not in stats[table_name]["V_a_r"]:
                            stats[table_name]["V_a_r"][col] = set()
                        stats[table_name]["V_a_r"][col].update(row[col] for row in block)
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
    
    def _evaluate_condition(self, row: Dict, condition: Condition):
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
