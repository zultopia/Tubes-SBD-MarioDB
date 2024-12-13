# Query Processor sangatlah mantap
# Import komponen yang lain dulu
from typing import List, Union, Dict, Optional

# Storage Manager
from StorageManager.classes import StorageManager, DataRetrieval, DataWrite, DataDeletion, Condition, ConditionGroup
from ConcurrencyControlManager.classes import ConcurrencyControlManager

# Query Optimizer
# untuk iterasi setiap node pada parse tree, dibutuhkan type matching setiap node
from QueryOptimizer.lexer import Token
# untuk proses parse tree
from QueryOptimizer.parse_tree import Node, ParseTree
# mengubah string query menjadi parse tree
from QueryOptimizer.query_optimizer import get_parse_tree
# mengubah parse tree menjadi query plan
from QueryOptimizer.query_optimizer import from_parse_tree

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
        # Dummy
        self.storage_manager.data = {
            "Student": [
                {"ID": 1, "name": "Alice", "dept_name": "Computer Science", "tot_cred": 120},
                {"ID": 2, "name": "Bob", "dept_name": "Mathematics", "tot_cred": 100},
                {"ID": 3, "name": "Charlie", "dept_name": "Physics", "tot_cred": 90},
            ],
            "Takes": [
                {"ID": 1, "course_id": 101, "sec_id": "A", "semester": "Fall", "year": 2023, "grade": "A"},
                {"ID": 2, "course_id": 102, "sec_id": "B", "semester": "Spring", "year": 2023, "grade": "B+"},
                {"ID": 3, "course_id": 103, "sec_id": "C", "semester": "Fall", "year": 2023, "grade": "A-"},
            ],
            "Section": [
                {"course_id": 101, "sec_id": "A", "semester": "Fall", "year": 2023, "building": "Science Hall", "room_no": 101, "time_slot_id": 1},
                {"course_id": 102, "sec_id": "B", "semester": "Spring", "year": 2023, "building": "Math Building", "room_no": 202, "time_slot_id": 2},
                {"course_id": 103, "sec_id": "C", "semester": "Fall", "year": 2023, "building": "Physics Lab", "room_no": 303, "time_slot_id": 3},
            ],
            "Time_Slot": [
                {"time_slot_id": 1, "day": "Monday", "start_time": "10:00", "end_time": "11:00"},
                {"time_slot_id": 2, "day": "Tuesday", "start_time": "12:00", "end_time": "13:30"},
                {"time_slot_id": 3, "day": "Wednesday", "start_time": "14:00", "end_time": "15:30"},
            ],
            "Course": [
                {"course_id": 101, "title": "Data Structures", "dept_name": "Computer Science", "credits": 3},
                {"course_id": 102, "title": "Calculus", "dept_name": "Mathematics", "credits": 4},
                {"course_id": 103, "title": "Quantum Mechanics", "dept_name": "Physics", "credits": 3},
            ],
            "Advisor": [
                {"s_id": 1, "i_id": 101},
                {"s_id": 2, "i_id": 102},
                {"s_id": 3, "i_id": 103},
            ],
            "Classroom": [
                {"building": "Science Hall", "room_no": 101, "capacity": 50},
                {"building": "Math Building", "room_no": 202, "capacity": 40},
                {"building": "Physics Lab", "room_no": 303, "capacity": 30},
            ],
            "Teaches": [
                {"ID": 101, "course_id": 101, "sec_id": "A", "semester": "Fall", "year": 2023},
                {"ID": 102, "course_id": 102, "sec_id": "B", "semester": "Spring", "year": 2023},
                {"ID": 103, "course_id": 103, "sec_id": "C", "semester": "Fall", "year": 2023},
            ],
            "Prereq": [
                {"course_id": 101, "prereq_id": 100},
                {"course_id": 102, "prereq_id": 101},
                {"course_id": 103, "prereq_id": 102},
            ],
            "Instructor": [
                {"ID": 101, "name": "Dr. Smith", "dept_name": "Computer Science", "salary": 90000},
                {"ID": 102, "name": "Dr. Johnson", "dept_name": "Mathematics", "salary": 85000},
                {"ID": 103, "name": "Dr. Lee", "dept_name": "Physics", "salary": 87000},
            ],
        }

    # buat testing, sudah ada query plan
    def test(self, query_string):
        query_tree = get_parse_tree(query_string)
        print(query_tree)
        query_plan = from_parse_tree(query_tree)
        print(query_plan)
    
    # currently working, belum pakai query plan, masih pakai query tree
    def execute_query(self, query_string: str) -> ExecutionResult:
        query_tree: ParseTree = get_parse_tree(query_string)
        # cek child pertama untuk menentukan jenis query (lengkapnya cek komentar dari fungsi di bawah ini)
        self.check_first_child(query_tree)
        print(query_tree)
        # query_plan belum jadi, sementara proses query tree dulu, lagipula nanti query plan juga bentuknya objek query tree, tapi teroptimasi

    # cek child pertama dari parse tree untuk menentukan jenis query SQL (SELECT, UPDATE, atau TRANSACTION)
    def check_first_child(self, query_tree: ParseTree):
        # child pertama adalah elemen pertama pada atribut childs dari ParseTree, berupa list of ParseTree
        first_child_node = query_tree.childs[0]
        # jika child pertama merupakan token SELECT
        if (isinstance(first_child_node.root, Node)) and (first_child_node.root.token_type in {Token.SELECT}):
            # panggil fungsi execute_SELECT
            self.execute_SELECT(query_tree)
        elif (isinstance(first_child_node.root, Node)) and (first_child_node.root.token_type) in (Token.UPDATE):
            # panggil fungsi execute_UPDATE
            self.execute_UPDATE(query_tree)

    # menambahkan atribut yang di-SELECT ke dalam schema dari node SelectListTail
    def add_schema_from_SelectListTail(self, SelectListTail_node: ParseTree, schema: Dict[str, List[str]]) -> Dict[str, List[str]]:
        # child kedua dari node SelectListTail adalah node Field
        field_node = SelectListTail_node.childs[1]
        # jika atribut hanya disebutkan namanya (contoh: SELECT id)
        if len(field_node.childs) == 1:
            # child pertama dari node Field adalah atribut pertama yang di-SELECT, tambahkan ke columns dengan key UNKNOWN karena nama tabel belum diketahui
            self.add_to_dictionary(schema, "UNKNOWN", field_node.childs[0].root.value)
        # jika menggunakan alias (contoh: SELECT t.id FROM table AS t)
        else:
            # child pertama dari node Field adalah alias tabel, child kedua adalah ., child ketiga adalah atributnya
            self.add_to_dictionary(schema, field_node.childs[0].root.value, field_node.childs[2].root.value)
        # jika tidak atribut lagi selanjutnya, kembalikan schema
        if (len(SelectListTail_node.childs) == 2):
            return schema
        # jika masih ada atribut selanjutnya yang di-SELECT, panggil fungsi secara rekursif
        else:
            return self.add_schema_from_SelectListTail(SelectListTail_node.childs[2], schema)

    # menambahkan key dan value ke sebuah dictionary
    def add_to_dictionary(self, dictionary: Union[Dict[str, List[str]]], key: str, value: str):
        # jika key sudah ada di dalam dictionary
        if key in dictionary:
            dictionary[key].append(value)
        # jika belum ada
        else:
            dictionary[key] = [value]

    # menempatkan nama asli tabel (beserta alias jika ada) dalam klausa FROM dalam dictionary schema
    # alias perlu disimpan karena klausa berikutnya (WHERE, ORDER BY, dll) mungkin menggunakan alias
    def execute_FROM(self, schema: Dict[str, List[str]], alias: Dict[str, str], query_tree: ParseTree):
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
            self.add_table_from_FromListTail(schema, alias, FromList_node.childs[1])
        # tabel-tabel dalam bentuk JOIN
        elif (len(FromList_node.childs[0].childs) == 2):
            self.add_table_from_TableResultTail(schema, alias, FromList_node.childs[0].childs[1])

    # menempatkan nama asli tabel (beserta alias jika ada) untuk tabel selain tabel pertama ke dalam schema
    # jika menggunakan bentuk comma separated
    def add_table_from_FromListTail(self, schema: Dict[str, List[str]], alias: Dict[str, str], FromListTail_node: ParseTree):
        # nama tabel disimpan dalam node TableTerm
        TableTerm_node = FromListTail_node.childs[1].childs[0]
        # jika menggunakan alias, langsung menggunakan nama asli tabel
        if len(TableTerm_node.childs) == 3:
            schema[TableTerm_node.childs[0].root.value] = schema.pop(TableTerm_node.childs[2].root.value)
            # tambahkan key berupa alias tabel dan value berupa nama asli tabel ke dalam dictionary
            alias[TableTerm_node.childs[2].root.value] = TableTerm_node.childs[0].root.value
        # jika tabel yang tersisa lebih dari 1
        if len(FromListTail_node.childs) == 3:
            self.add_table_from_FromListTail(schema, alias, FromListTail_node.childs[2])

    # menempatkan nama asli tabel (beserta alias jika ada) untuk tabel selain tabel pertama ke dalam schema
    # jika menggunakan bentuk JOIN
    def add_table_from_TableResultTail(self, schema: Dict[str, List[str]], alias: Dict[str, str], TableResultTail_node: ParseTree):
        # nama tabel disimpan dalam node TableTerm
        TableTerm_node = TableResultTail_node.childs[2]
        if len(TableTerm_node.childs) == 3:
            schema[TableTerm_node.childs[0].root.value] = schema.pop(TableTerm_node.childs[2].root.value)
            # tambahkan key berupa alias tabel dan value berupa nama asli tabel ke dalam dictionary
            alias[TableTerm_node.childs[2].root.value] = TableTerm_node.childs[0].root.value
        # jika tabel yang tersisa lebih dari 1
        if len(TableResultTail_node.childs) == 4:
            self.add_table_from_FromListTail(schema, alias, TableResultTail_node.childs[3])

    # fungsi untuk ORDER BY dan LIMIT
    def execute_ORDER_BY(self, rows: List[Dict[str, Union[int, str]]], order_by_attr: str, descending: bool = False) -> List[Dict[str, Union[int, str]]]:
        def get_sort_key(row):
            value = row.get(order_by_attr, None)
            if isinstance(value, str):
                return tuple(ord(char) for char in value)
            return value
        return sorted(rows, key=get_sort_key, reverse=descending)

    def execute_LIMIT(rows: List[Dict[str, Union[int, str]]], limit: int) -> List[Dict[str, Union[int, str]]]:
        return rows[:limit]

    # handle query SELECT, menerima input node query tree
    def execute_SELECT(self, query_tree: ParseTree):
        # child kedua dari query tree adalah node SelectList
        SelectList_node = query_tree.childs[1]
        # child pertama dari node SelectList adalah node Field
        field_node = SelectList_node.childs[0]
        # inisialisasi Dict[str, List[str]] untuk menyimpan atribut apa saja yang di-SELECT
        retrieval_schema = {}

        # jika atribut hanya disebutkan namanya (contoh: SELECT id)
        if len(field_node.childs) == 1:
            # child pertama dari node Field adalah atribut pertama yang di-SELECT, tambahkan ke columns dengan key UNKNOWN karena nama tabel belum diketahui
            self.add_to_dictionary(retrieval_schema, "UNKNOWN", field_node.childs[0].root.value)
        # jika menggunakan alias (contoh: SELECT t.id FROM table AS t)
        else:
            # child pertama dari node Field adalah alias tabel, child kedua adalah ., child ketiga adalah atributnya
            self.add_to_dictionary(retrieval_schema, field_node.childs[0].root.value, field_node.childs[2].root.value)
        # kalau atribut yang di-SELECT lebih dari 1
        if len(SelectList_node.childs) > 1:
            retrieval_schema = self.add_schema_from_SelectListTail(SelectList_node.childs[1], retrieval_schema)
        # inisialisasi dictionary untuk menyimpan alias tabel
        alias = {}
        # objek ConditionGroup untuk menyimpan semua kondisi
        conditions = ConditionGroup([], "")
        # tambahkan nama tabel ke dalam skema dari node FromList
        self.execute_FROM(retrieval_schema, alias, query_tree)
        # Testing (DELETE LATER)
        print("schema =", retrieval_schema, "\n")
        print("alias =", alias, "\n")
        # jika ada klausa berikutnya
        if len(query_tree.childs) >= 6:
            # jika diikuti klausa WHERE
            if query_tree.childs[5].root == "Condition":
                # self.execute_WHERE(retrieval_schema, alias, conditions, query_tree.childs[5])
                pass
            # jika diikuti klausa ORDER BY
            elif query_tree.childs[5].root == "ORDER_BY":
                order_by_node = query_tree.childs[6]
                order_by_attr = order_by_node.childs[1].root.value
                descending = len(order_by_node.childs) > 2 and order_by_node.childs[2].root.value.upper() == "DESC"
                rows = self.execute_ORDER_BY(rows, order_by_attr, descending)
            # jika diikuti klausa LIMIT
            elif query_tree.childs[5].root == "LIMIT":
                limit_node = query_tree.childs[7]
                limit_value = int(limit_node.childs[1].root.value)
                rows = self.execute_LIMIT(rows, limit_value)
    
        # Mengambil dari StorageManager
        rows = []
        for table, attributes in retrieval_schema.items() :
            print(table)
            print(attributes)
            
            data_retrieval = DataRetrieval(table, attributes, conditions, None, None)
            rows.append(self.storage_manager.read_block(data_retrieval))
        
        print(rows)
        print("\n")
    
    def execute_WHERE(self, schema: Dict[str, List[str]], alias: Dict[str, str], conditionGroup: ConditionGroup, Condition_node: ParseTree):
        # inisialisasi conditions
        conditions = []
        # klausa "AND"
        if len(Condition_node.childs) == 1:
            logic_operator = "AND"
            AndCondition_node = Condition_node.childs[0]
            ConditionTerm_node = AndCondition_node.childs[0]
            AndConditionTail_node = AndCondition_node.childs[1]
        
    # TODO (menunggu kelompok query optimizer)
    # handle query UPDATE, menerima input node query tree
    def execute_UPDATE(self, query_tree: ParseTree):
        pass

    # TODO (menunggu kelompok query optimizer)
    # handle query BEGIN TRANSACTION, menerima input query tree
    def execute_BEGIN_TRANSCATION(self, query_tree: ParseTree):
        pass