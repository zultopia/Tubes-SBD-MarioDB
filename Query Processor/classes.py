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
    def __init__(self, transaction_id, timestamp, message, data, query):
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
        self.execute_query(query)
        pass
    
    def execute_query(query):
        # Mengirimkan query transaction ke Concurrency Control Manager
        # Mengirimkan query awal ke Query Optimizer, menghasilkan Parsed Query
        parse_tree = self.parse_query(query)
        # Menerima Parse Query akan di optimasi oleh Query Optimizer
        # Menjalankan query plan ke Storage Manager
        # Menerima dan mengirimkan ExecutionResult ke user
        pass
    
    # Fungsi yang berhubungan dengan storage manager
    # Fungsi yang melakukan parsing dengan memanggil Query Optimizer
    def parse_query(query):
        parse_tree = parse(query)
        return parse_tree        
    
    # Fungsi yang berhubungan dengan storage manager
    # Fungsi yang dapat mengeksekusi SELECT, FROM, WHERE ke storage manager
    def execute_read(data_retrieval):
        pass
        
