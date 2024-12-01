# Query Processor sangatlah mantap
# Import komponen yang lain dulu
from typing import List, Union, Dict, Optional

# Storage Manager
from StorageManager.classes import StorageManager, DataRetrieval, DataWrite, DataDeletion, Condition

# Query Optimizer
# untuk iterasi setiap node pada parse tree, dibutuhkan type matching setiap node
from QueryOptimizer.lexer import Token
# untuk proses parse tree
from QueryOptimizer.parse_tree import Node, ParseTree
# mengubah string query menjadi parse tree
from QueryOptimizer.query_optimizer import get_parse_tree
# mengubah parse tree menjadi query plan
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
    def execute_query(self, query_string: str) -> ExecutionResult:
        query_tree: ParseTree = get_parse_tree(query_string)
        # cek child pertama untuk menentukan jenis query (lengkapnya cek komentar dari fungsi di bawah ini)
        schema = check_first_child(query_tree)
        # tambahkan nama tabel ke dalam skema dari node FromList
        add_table_to_schema(schema, query_tree)
        # Testing
        print("schema =", schema, "\n")
        print(query_tree)
        # query_plan belum jadi, sementara proses query tree dulu, lagipula nanti query plan juga bentukna objek query tree, tapi teroptimasi

# cek child pertama dari parse tree untuk menentukan jenis query SQL (SELECT, UPDATE, atau TRANSACTION)
def check_first_child(query_tree: ParseTree):
    # child pertama adalah elemen pertama pada atribut childs dari ParseTree, berupa list of ParseTree
    first_child_node = query_tree.childs[0]
    # jika child pertama merupakan token SELECT
    if (isinstance(first_child_node.root, Node)) and (first_child_node.root.token_type in {Token.SELECT}):
        # child selanjutnya adalah node SelectList
        SelectList_node = query_tree.childs[1]
        # child pertama dari node SelectList adalah node Field
        field_node = SelectList_node.childs[0]
        # inisialisasi Dict[str, List[str]] untuk menyimpan atribut apa saja yang di-SELECT
        retrieval_schema = {}

        # jika atribut hanya disebutkan namanya (contoh: SELECT id)
        if len(field_node.childs) == 1:
            # child pertama dari node Field adalah atribut pertama yang di-SELECT, tambahkan ke columns dengan key UNKNOWN karena nama tabel belum diketahui
            add_to_schema(retrieval_schema, "UNKNOWN", field_node.childs[0].root.value)
        # jika menggunakan alias (contoh: SELECT t.id FROM table AS t)
        else:
            # child pertama dari node Field adalah alias tabel, child kedua adalah ., child ketiga adalah atributnya
            add_to_schema(retrieval_schema, field_node.childs[0].root.value, field_node.childs[2].root.value)
        # kalau atribut yang di-SELECT cuma 1
        if len(SelectList_node.childs) == 1:
            return retrieval_schema
        # kalau atribut yang di-SELECT lebih dari 1
        else:
            return add_schema_from_SelectListTail(SelectList_node.childs[1], retrieval_schema)
    else:
        return {}

# menambahkan atribut yang di-SELECT ke dalam schema dari node SelectListTail
def add_schema_from_SelectListTail(SelectListTail_node: ParseTree, schema: Dict[str, List[str]]):
    # child kedua dari node SelectListTail adalah node Field
    field_node = SelectListTail_node.childs[1]
    # jika atribut hanya disebutkan namanya (contoh: SELECT id)
    if len(field_node.childs) == 1:
        # child pertama dari node Field adalah atribut pertama yang di-SELECT, tambahkan ke columns dengan key UNKNOWN karena nama tabel belum diketahui
        add_to_schema(schema, "UNKNOWN", field_node.childs[0].root.value)
    # jika menggunakan alias (contoh: SELECT t.id FROM table AS t)
    else:
        # child pertama dari node Field adalah alias tabel, child kedua adalah ., child ketiga adalah atributnya
        add_to_schema(schema, field_node.childs[0].root.value, field_node.childs[2].root.value)
    # jika tidak atribut lagi selanjutnya, kembalikan schema
    if len(SelectListTail_node.childs) == 2:
        return schema
    # jika masih ada atribut selanjutnya yang di-SELECT, panggil fungsi secara rekursif
    else:
        return add_schema_from_SelectListTail(SelectListTail_node.childs[2], schema)

# menambahkan tabel dan atribut yang di-SELECT ke sebuah dictionary yang menyimpan skema
def add_to_schema(schema: Dict[str, List[str]], table: str, column: str):
    # jika tabel sudah ada di dalam dictionary
    if table in schema:
        schema[table].append(column)
    # jika belum ada
    else:
        schema[table] = [column]

# menempatkan nama asli tabel dalam klausa FROM dalam dictionary schema
def add_table_to_schema(schema: Dict[str, List[str]], query_tree: ParseTree):
    # tabel pertama dalam klausa FROM
    FromList_node = query_tree.childs[3]
    # nama tabel pertama disimpan dalam node TableTerm
    TableTerm_node = FromList_node.childs[0].childs[0]
    # jika tidak menggunakan alias, langsung menggunakan nama asli tabel
    if len(TableTerm_node.childs) == 1:
        # ganti key "UNKNOWN" pada dictionary schema menjadi nama tabel
        schema[TableTerm_node.childs[0].root.value] = schema.pop("UNKNOWN")
    # jika menggunakan alias
    else:
        # ganti key alias tabel dengan nama asli tabel
        schema[TableTerm_node.childs[0].root.value] = schema.pop(TableTerm_node.childs[2].root.value)
    # jika tabel dalam klausa FROM lebih dari 1
    if len(FromList_node.childs) == 2:
        add_table_from_FromListTail(schema, FromList_node.childs[1])

# menempatkan nama asli tabel untuk tabel selain tabel pertama ke dalam schema
def add_table_from_FromListTail(schema: Dict[str, List[str]], FromListTail_node: ParseTree):
    # nama tabel disimpan dalam node TableTerm
    TableTerm_node = FromListTail_node.childs[1].childs[0]
    # jika menggunakan alias, langsung menggunakan nama asli tabel
    if len(TableTerm_node.childs) == 3:
        schema[TableTerm_node.childs[0].root.value] = schema.pop(TableTerm_node.childs[2].root.value)
    # jika tabel yang tersisa lebih dari 1
    if len(FromListTail_node.childs) == 3:
        add_table_from_FromListTail(schema, FromListTail_node.childs[2])