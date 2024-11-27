# Query Processor sangatlah mantap
# Import komponen yang lain dulu
from typing import List, Union, Dict, Optional

# Storage Manager
from StorageManager.classes import StorageManager
# Query Optimizer
from QueryOptimizer.parse_tree import ParseTree
from QueryOptimizer.query_optimizer import get_parse_tree
from QueryOptimizer.from_parse_tree import from_parse_tree

# Kita buat class interface untuk objek-objek yang digunakan
class Rows:
    def __init__(self, data: Dict[str, List[Dict[str, Union[int, str]]]], rows_count: int):
        self.data = data
        self.rows_count = rows_count

class ExecutionResult:
    def __init__(self, transaction_id: int, timestamp: str, message: str, data_before: Optional[Union[Rows, int]], data_after: Optional[Union[Rows, int]], status: str, query: str):
        self.transaction_id = transaction_id
        self.timestamp = timestamp
        self.message = message
        self.data_before = data_before
        self.data_after = data_after
        self.status = status
        self.query = query

# Sekarang query processornya
class QueryProcessor:
    def __init__(self):
        # Instansiasi semua komponen
        # Instansiasi Query Processor
        # Instansiasi Concurrency Control Manager
        # Intansiasi Storage Manager
        self.storage_manager = StorageManager()
    def execute_query(self):
        query_string = input('Please enter your query: ')
        tree = get_parse_tree(query_string)
        query_plan = from_parse_tree(tree)
        query_plan.print()
        print(tree)