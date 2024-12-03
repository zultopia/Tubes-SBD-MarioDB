import os
import pickle
import math
from typing import List, fasdfasdfUnion

class Condition:
    def __init__(self, column: str, operation: str, operand: Union[int, str]):
        self.column = column
        self.operation = operation
        self.operand = operand

class DataRetrieval:
    def __init__(self, table: str, columns: List[str], conditions: List[Condition], search_type: str):
        self.table = table
        self.columns = columns
        self.conditions = conditions
        self.search_type = search_type

class DataWrite:
    def __init__(self, table: str, columns: List[str], new_values: List[Union[int, str]], conditions: List[Condition] = None):
        self.table = table
        self.columns = columns
        self.new_values = new_values
        self.conditions = conditions or []

class DataDeletion:
    def __init__(self, table: str, conditions: List[Condition]):
        self.table = table
        self.conditions = conditions

class Statistic:
    def __init__(self, n_r: int, b_r: int, l_r: int, f_r: int, V_a_r: dict):
        self.n_r = n_r
        self.b_r = b_r
        self.l_r = l_r
        self.f_r = f_r
        self.V_a_r = V_a_r

class StorageManager:
    DATA_FILE = "data.dat"

    def __init__(self):
        self.data = self._load_data()
        self.indexes = {}

    def _load_data(self):
        if os.path.exists(self.DATA_FILE):
            with open(self.DATA_FILE, "rb") as file:
                return pickle.load(file)
        return {}

    def _save_data(self):
        with open(self.DATA_FILE, "wb") as file:
            pickle.dump(self.data, file)

    def read_block(self, data_retrieval: DataRetrieval):
        table = self.data.get(data_retrieval.table, [])
        result = []
        for row in table:
            if all(self._evaluate_condition(row, cond) for cond in data_retrieval.conditions):
                result.append({col: row[col] for col in data_retrieval.columns})
        return result

    def write_block(self, data_write: DataWrite):
        table = self.data.setdefault(data_write.table, [])
        affected_rows = 0
        if data_write.conditions:
            for row in table:
                if all(self._evaluate_condition(row, cond) for cond in data_write.conditions):
                    for col, val in zip(data_write.columns, data_write.new_values):
                        row[col] = val
                    affected_rows += 1
        else:
            new_row = {col: val for col, val in zip(data_write.columns, data_write.new_values)}
            table.append(new_row)
            affected_rows += 1
        self._save_data()
        return affected_rows

    def delete_block(self, data_deletion: DataDeletion):
        table = self.data.get(data_deletion.table, [])
        initial_size = len(table)
        table = [row for row in table if not all(self._evaluate_condition(row, cond) for cond in data_deletion.conditions)]
        self.data[data_deletion.table] = table
        self._save_data()
        return initial_size - len(table)

    def set_index(self, table: str, column: str, index_type: str):
        if index_type != "hash":
            raise ValueError("Index yang digunakan adalah hash index.")
        
        if table not in self.data:
            raise ValueError(f"Table {table} tidak ditemukan.")
        
        # Membuat indeks berbasis hash
        self.index[(table, column)] = {}
        for row in self.data[table]:
            key = row[column]
            if key not in self.index[(table, column)]:
                self.index[(table, column)][key] = []
            self.index[(table, column)][key].append(row)

        print(f"Hash index dibuat pada {table}.{column}")

    def read_block_with_index(self, table: str, column: str, value):
        # Gunain indeks kalo ada
        index_key = (table, column)
        if index_key in self.index:
            return self.index[index_key].get(value, [])
        else:
            raise ValueError(f"Indeks tidak ditemukan untuk {table}.{column}.")
        
    def get_stats(self):
        stats = {}
        for table_name, table in self.data.items():
            n_r = len(table)
            l_r = len(table[0]) if table else 0
            f_r = math.floor(4096 / l_r) if l_r else 0  # Assume block size = 4096 bytes
            b_r = math.ceil(n_r / f_r) if f_r else 0
            V_a_r = {col: len(set(row[col] for row in table)) for col in table[0]} if table else {}
            stats[table_name] = Statistic(n_r, b_r, l_r, f_r, V_a_r)
        return stats

    def _evaluate_condition(self, row, condition: Condition):
        value = row.get(condition.column)
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