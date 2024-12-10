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
        check_first_child(query_tree)
        print(query_tree)
        # query_plan belum jadi, sementara proses query tree dulu, lagipula nanti query plan juga bentuknya objek query tree, tapi teroptimasi

# cek child pertama dari parse tree untuk menentukan jenis query SQL (SELECT, UPDATE, atau TRANSACTION)
def check_first_child(query_tree: ParseTree):
    # child pertama adalah elemen pertama pada atribut childs dari ParseTree, berupa list of ParseTree
    first_child_node = query_tree.childs[0]
    # jika child pertama merupakan token SELECT
    if (isinstance(first_child_node.root, Node)) and (first_child_node.root.token_type in {Token.SELECT}):
        # panggil fungsi execute_SELECT
        execute_SELECT(query_tree)
    elif (isinstance(first_child_node.root, Node)) and (first_child_node.root.token_type) in (Token.UPDATE):
        # panggil fungsi execute_UPDATE
        execute_UPDATE(query_tree)

# menambahkan atribut yang di-SELECT ke dalam schema dari node SelectListTail
def add_schema_from_SelectListTail(SelectListTail_node: ParseTree, schema: Dict[str, List[str]]) -> Dict[str, List[str]]:
    # child kedua dari node SelectListTail adalah node Field
    field_node = SelectListTail_node.childs[1]
    # jika atribut hanya disebutkan namanya (contoh: SELECT id)
    if len(field_node.childs) == 1:
        # child pertama dari node Field adalah atribut pertama yang di-SELECT, tambahkan ke columns dengan key UNKNOWN karena nama tabel belum diketahui
        add_to_dictionary(schema, "UNKNOWN", field_node.childs[0].root.value)
    # jika menggunakan alias (contoh: SELECT t.id FROM table AS t)
    else:
        # child pertama dari node Field adalah alias tabel, child kedua adalah ., child ketiga adalah atributnya
        add_to_dictionary(schema, field_node.childs[0].root.value, field_node.childs[2].root.value)
    # jika tidak atribut lagi selanjutnya, kembalikan schema
    if (len(SelectListTail_node.childs) == 2):
        return schema
    # jika masih ada atribut selanjutnya yang di-SELECT, panggil fungsi secara rekursif
    else:
        return add_schema_from_SelectListTail(SelectListTail_node.childs[2], schema)

# menambahkan key dan value ke sebuah dictionary
def add_to_dictionary(dictionary: Union[Dict[str, List[str]] | Dict[str, List[Condition]]], key: str, value: Union[str | Condition]):
    # jika key sudah ada di dalam dictionary
    if key in dictionary:
        dictionary[key].append(value)
    # jika belum ada
    else:
        dictionary[key] = [value]

# menempatkan nama asli tabel (beserta alias jika ada) dalam klausa FROM dalam dictionary schema
# alias perlu disimpan karena klausa berikutnya (WHERE, ORDER BY, dll) mungkin menggunakan alias
def execute_FROM(schema: Dict[str, List[str]], alias: Dict[str, str], query_tree: ParseTree):
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
        # tambahkan key berupa alias tabel dan value berupa nama asli tabel ke dalam dictionary
        alias[TableTerm_node.childs[2].root.value] = TableTerm_node.childs[0].root.value
    # jika tabel dalam klausa FROM lebih dari 1
    # tabel-tabel dalam bentuk terpisah dengan koma
    if (len(FromList_node.childs) == 2):
        add_table_from_FromListTail(schema, alias, FromList_node.childs[1])
    # tabel-tabel dalam bentuk JOIN
    elif (len(FromList_node.childs[0].childs) == 2):
        add_table_from_TableResultTail(schema, alias, FromList_node.childs[0].childs[1])

# menempatkan nama asli tabel (beserta alias jika ada) untuk tabel selain tabel pertama ke dalam schema
# jika menggunakan bentuk comma separated
def add_table_from_FromListTail(schema: Dict[str, List[str]], alias: Dict[str, str], FromListTail_node: ParseTree):
    # nama tabel disimpan dalam node TableTerm
    TableTerm_node = FromListTail_node.childs[1].childs[0]
    # jika menggunakan alias, langsung menggunakan nama asli tabel
    if len(TableTerm_node.childs) == 3:
        schema[TableTerm_node.childs[0].root.value] = schema.pop(TableTerm_node.childs[2].root.value)
        # tambahkan key berupa alias tabel dan value berupa nama asli tabel ke dalam dictionary
        alias[TableTerm_node.childs[2].root.value] = TableTerm_node.childs[0].root.value
    # jika tabel yang tersisa lebih dari 1
    if len(FromListTail_node.childs) == 3:
        add_table_from_FromListTail(schema, alias, FromListTail_node.childs[2])

# menempatkan nama asli tabel (beserta alias jika ada) untuk tabel selain tabel pertama ke dalam schema
# jika menggunakan bentuk JOIN
def add_table_from_TableResultTail(schema: Dict[str, List[str]], alias: Dict[str, str], TableResultTail_node: ParseTree):
    # nama tabel disimpan dalam node TableTerm
    TableTerm_node = TableResultTail_node.childs[2]
    if len(TableTerm_node.childs) == 3:
        schema[TableTerm_node.childs[0].root.value] = schema.pop(TableTerm_node.childs[2].root.value)
        # tambahkan key berupa alias tabel dan value berupa nama asli tabel ke dalam dictionary
        alias[TableTerm_node.childs[2].root.value] = TableTerm_node.childs[0].root.value
    # jika tabel yang tersisa lebih dari 1
    if len(TableResultTail_node.childs) == 4:
        add_table_from_FromListTail(schema, alias, TableResultTail_node.childs[3])

# handle query SELECT, menerima input node query tree
def execute_SELECT(query_tree: ParseTree):
    # child kedua dari query tree adalah node SelectList
    SelectList_node = query_tree.childs[1]
    # child pertama dari node SelectList adalah node Field
    field_node = SelectList_node.childs[0]
    # inisialisasi Dict[str, List[str]] untuk menyimpan atribut apa saja yang di-SELECT
    retrieval_schema = {}

    # jika atribut hanya disebutkan namanya (contoh: SELECT id)
    if len(field_node.childs) == 1:
        # child pertama dari node Field adalah atribut pertama yang di-SELECT, tambahkan ke columns dengan key UNKNOWN karena nama tabel belum diketahui
        add_to_dictionary(retrieval_schema, "UNKNOWN", field_node.childs[0].root.value)
    # jika menggunakan alias (contoh: SELECT t.id FROM table AS t)
    else:
        # child pertama dari node Field adalah alias tabel, child kedua adalah ., child ketiga adalah atributnya
        add_to_dictionary(retrieval_schema, field_node.childs[0].root.value, field_node.childs[2].root.value)
    # kalau atribut yang di-SELECT lebih dari 1
    if len(SelectList_node.childs) > 1:
        retrieval_schema = add_schema_from_SelectListTail(SelectList_node.childs[1], retrieval_schema)
    # inisialisasi dictionary untuk menyimpan alias tabel
    alias = {}
    # dictionary untuk menyimpan kondisi setiap tabel
    conditions: Dict[str, List[Condition]] = {}
    # tambahkan nama tabel ke dalam skema dari node FromList
    execute_FROM(retrieval_schema, alias, query_tree)
    # Testing (DELETE LATER)
    print("schema =", retrieval_schema, "\n")
    print("alias =", alias, "\n")
    # jika ada klausa berikutnya
    if len(query_tree.childs) >= 6:
        # jika ada klausa WHERE
            if query_tree.childs[5].root == "Condition":
                execute_WHERE(retrieval_schema, alias, conditions, query_tree)
                for table, conditions in conditions.items():
                    print(f"Table: {table}")
                    for condition in conditions:
                        print(f"  - {condition.column} {condition.operation} {condition.operand}")

def add_condition_from_AndConditionTail(schema: Dict[str, List[str]], alias: Dict[str, str], conditions: Dict[str, List[Condition]], AndConditionTail_node: ParseTree):
    ConditionTerm_node = AndConditionTail_node.childs[1]
    Field_node = ConditionTerm_node.childs[0]
    # jika tidak menggunakan alias
    if len(Field_node.childs) == 1:
        table = next[iter(conditions)]
        column = Field_node.childs[0].root.value
    # jika menggunakan alias
    else:
        table_alias = Field_node.childs[0].root.value
        table = alias[table_alias]
        column = Field_node.childs[2].root.value
    operation = ConditionTerm_node.childs[1].childs[0].root.value
    operand = ConditionTerm_node.childs[2].root.value
    # tambahkan kondisi ke dictionary
    add_to_dictionary(conditions, table, Condition(column, operation, operand))
    # jika ada kondisi lain
    if len(AndConditionTail_node.childs) == 3:
        AndConditionTail_node = AndConditionTail_node.childs[2]
        add_condition_from_AndConditionTail(schema, alias, conditions, AndConditionTail_node)

def execute_WHERE(schema: Dict[str, List[str]], alias: Dict[str, str], conditions: Dict[str, List[Condition]], query_tree: ParseTree) -> Dict[str, List[Condition]]:
    Condition_node = query_tree.childs[5]
    AndCondition_node = Condition_node.childs[0]
    ConditionTerm_node = AndCondition_node.childs[0]
    Field_node = ConditionTerm_node.childs[0]
    # jika tidak menggunakan alias
    if len(Field_node.childs) == 1:
        table = next[iter(conditions)]
        column = Field_node.childs[0].root.value
    # jika menggunakan alias
    else:
        table_alias = Field_node.childs[0].root.value
        table = alias[table_alias]
        column = Field_node.childs[2].root.value
    operation = ConditionTerm_node.childs[1].childs[0].root.value
    operand = ConditionTerm_node.childs[2].root.value
    # tambahkan kondisi ke dictionary
    add_to_dictionary(conditions, table, Condition(column, operation, operand))
    # jika ada kondisi lain
    if len(AndCondition_node.childs) == 2:
        AndConditionTail_node = AndCondition_node.childs[1]
        add_condition_from_AndConditionTail(schema, alias, conditions, AndConditionTail_node)
    return conditions

# TODO (sekarang)
# fungsi untuk ORDER BY dan LIMIT

# TODO (menunggu kelompok query optimizer)
# handle query UPDATE, menerima input node query tree
def execute_UPDATE(query_tree: ParseTree):
    pass

# TODO (menunggu kelompok query optimizer)
# handle query BEGIN TRANSACTION, menerima input query tree
def execute_BEGIN_TRANSCATION(query_tree: ParseTree):
    pass