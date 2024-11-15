# Query Processor sangatlah mantap

# Kita buat class interface untuk objek-objek yang digunakan
# Nanti coba tanya, kalau import librarynya untuk typing boleh nggak
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

# Sekarang query processornya (sekarang masih belum ada apa apa)
class QueryProcessor:
    # Fungsi ini melakukan:
    # 1. Mengirimkan query awal ke Query Optimization
    # 2. Mengirimkan query transaction ke Concurrency Control Manager
    # 3. Mengirimkan query plan ke Storage Manager
    # 4. Menerima dan mengirimkan ExecutionResult ke user
    def execute_query(query):
        pass
