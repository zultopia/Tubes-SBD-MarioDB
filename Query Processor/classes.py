# Query Processor sangatlah mantap
# Import komponen yang lain dulu
import sys
import os
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.extend([
    project_root,
    os.path.join(project_root, 'query-optimizer')
])

# Storage Manager
from StorageManager.classes import StorageManager
# Query Optimizer
from parser import parse

# Kita buat class interface untuk objek-objek yang digunakan
class Rows:
    def __init__(self, data, rows_count):
        self.data = data
        self.rows_count = rows_count

class ExecutionResult:
    def __init__(self, transaction_id, timestamp, message, data, query, manager):
        self.transaction_id = transaction_id
        self.timestamp = timestamp
        self.message = message
        self.data = data
        self.query = query

# Sekarang query processornya
class QueryProcessor:
    def __init__(self, query):
        # Instansiasi semua komponen
        # Instansiasi Query Processor
        # Instansiasi Storage Manager
        # Instansiasi Concurrency Control Manager
        # Instansiasi query
        self.transaction_id = -1 # placeholder value, belum tau transaction id untuk semua query atau hanya query transaction
        self.query = query
        self.storage_manager = StorageManager()
        # Placeholder, belum pakai data dari file sesungguhnya
        self.storage_manager.data = {
            "Student": [
                {"StudentID": 1, "FullName": "Alice", "GPA": 3.5},
                {"StudentID": 2, "FullName": "Bob", "GPA": 3.8},
            ]
        }
    
    # Setter getter
    # Awalnya hanya dibuat untuk kepentingan testing
    def set_query(self, query):
        self.query = query
        
    def get_query(self):
        return self.query
    
    # Intinya deh
    def execute_query(self):
        # parse query
        parse_tree = self.parse_query()
        # cari node select
        select_list_node = parse_tree.find("SelectList")
        columns = self._extract_columns(select_list_node)
        # cari node from
        from_list_node = parse_tree.find("FromList")
        table, alias = self._extract_table_and_alias(from_list_node)
        # bentuk obyek DataRetrieval untuk storage manager
        data_retrieval = DataRetrieval(
            table=[table],  # List format expected
            columns=columns,
            conditions=[]
        )
        # ambil data menggunakan storage manager
        rows = self.storage_manager.read_block(data_retrieval)
        # handling jika query menggunakan alias untuk nama tabel
        if alias:
            rows.data = [
                {f"{alias}.{col}": value for col, value in row.items()}
                for row in rows.data
            ]
        # bentuk obyek executionresult sebagai hasil eksekusi query
        result = ExecutionResult(
            transaction_id=self.transaction_id,
            timestamp="2024-11-22 12:00:00",  # placeholder value, nanti diganti
            message="Query executed successfully", # placeholder, belum handle error messgae
            data=rows,
            query=self.query
        )
        print(result)
        return result

    # parse query menggunakan query optimizer
    def parse_query(self):
        return parse(self.query)

    # fungsi-fungsi pembantu untuk mengolah node sebelum pemrosesan
    def _extract_columns(self, select_list_node):
        columns = []
        for field_node in select_list_node.children:
            if field_node.root == "Field":
                column = self._extract_column_name(field_node)
                columns.append(column)
        return columns

    def _extract_column_name(self, field_node):
        children = field_node.children
        if len(children) == 1:
            return children[0].value
        elif len(children) == 3:
            return f"{children[0].value}.{children[2].value}"

    def _extract_table_and_alias(self, from_list_node):
        table_ref_node = from_list_node.find("TableReference")
        table = table_ref_node.children[0].value
        alias = None
        if len(table_ref_node.children) > 1:
            alias = table_ref_node.children[2].value
        return table, alias
        
    # Fungsi yang berhubungan dengan storage manager
    # Fungsi yang melakukan parsing dengan memanggil Query Optimizer
    def parse_query(self):
        parse_tree = parse(self.query)
        return parse_tree