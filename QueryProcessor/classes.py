# Query Processor sangatlah mantap
# Import komponen yang lain dulu
from typing import List, Union, Dict, Optional

# Storage Manager
from StorageManager.classes import StorageManager, DataRetrieval, DataWrite, DataDeletion, Condition
from ConcurrencyControlManager.classes import ConcurrencyControlManager

# Query Optimizer
# untuk iterasi setiap node pada parse tree, dibutuhkan type matching setiap node
from QueryOptimizer.lexer import Token
# untuk proses parse tree
from QueryOptimizer.parse_tree import Node, ParseTree
# mengubah string query menjadi parse tree
from QueryOptimizer.query_optimizer import get_parse_tree
# mengubah parse tree menjadi query plan
from QueryOptimizer.from_parse_tree import from_parse_tree

# import library lain yang diperlukan
import datetime

# Kita buat class interface untuk objek-objek yang digunakan
class Rows:
    def __init__(self, data: List[Dict[str, Union[int, str]]]):
        self.data = data
        self.rows_count = len(data)

class ExecutionResult:
    def __init__(self, transaction_id: int, timestamp: str, message: str, data: Rows, query: str):
        self.transaction_id = transaction_id
        self.timestamp = timestamp
        self.message = message
        self.data = data
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
                {"StudentID": 1, "FullName": "Alice Johnson", "GPA": 3.8},
                {"StudentID": 2, "FullName": "Bob Smith", "GPA": 3.2},
                {"StudentID": 3, "FullName": "Charlie Brown", "GPA": 3.6},
                {"StudentID": 4, "FullName": "Diana Prince", "GPA": 3.9},
                {"StudentID": 5, "FullName": "Ethan Hunt", "GPA": 3.7},
                {"StudentID": 6, "FullName": "Fiona Apple", "GPA": 3.4},
                {"StudentID": 7, "FullName": "George Miller", "GPA": 3.1},
                {"StudentID": 8, "FullName": "Hannah Montana", "GPA": 3.5},
                {"StudentID": 9, "FullName": "Ivan Drago", "GPA": 3.3},
                {"StudentID": 10, "FullName": "Jane Doe", "GPA": 3.0},
                {"StudentID": 11, "FullName": "Kevin Hart", "GPA": 3.6},
                {"StudentID": 12, "FullName": "Laura Croft", "GPA": 3.8},
                {"StudentID": 13, "FullName": "Michael Scott", "GPA": 3.2},
                {"StudentID": 14, "FullName": "Nancy Drew", "GPA": 3.7},
                {"StudentID": 15, "FullName": "Oscar Wilde", "GPA": 3.9},
                {"StudentID": 16, "FullName": "Paul Rudd", "GPA": 3.4},
                {"StudentID": 17, "FullName": "Quinn Harper", "GPA": 3.1},
                {"StudentID": 18, "FullName": "Rachel Green", "GPA": 3.5},
                {"StudentID": 19, "FullName": "Steve Rogers", "GPA": 3.3},
                {"StudentID": 20, "FullName": "Tony Stark", "GPA": 3.9},
                {"StudentID": 21, "FullName": "Uma Thurman", "GPA": 3.7},
                {"StudentID": 22, "FullName": "Victor Creed", "GPA": 3.2},
                {"StudentID": 23, "FullName": "Wanda Maximoff", "GPA": 3.8},
                {"StudentID": 24, "FullName": "Xander Cage", "GPA": 3.5},
                {"StudentID": 25, "FullName": "Yvonne Strahovski", "GPA": 3.6},
                {"StudentID": 26, "FullName": "Zachary Levi", "GPA": 3.4},
                {"StudentID": 27, "FullName": "Alan Grant", "GPA": 3.1},
                {"StudentID": 28, "FullName": "Barbara Gordon", "GPA": 3.9},
                {"StudentID": 29, "FullName": "Caleb Rivers", "GPA": 3.3},
                {"StudentID": 30, "FullName": "Dana Scully", "GPA": 3.7},
                {"StudentID": 31, "FullName": "Elliot Stabler", "GPA": 3.8},
                {"StudentID": 32, "FullName": "Francis Underwood", "GPA": 3.0},
                {"StudentID": 33, "FullName": "Gillian Flynn", "GPA": 3.5},
                {"StudentID": 34, "FullName": "Hank Pym", "GPA": 3.6},
                {"StudentID": 35, "FullName": "Isabel Lucas", "GPA": 3.3},
                {"StudentID": 36, "FullName": "John Wick", "GPA": 3.9},
                {"StudentID": 37, "FullName": "Kara Danvers", "GPA": 3.8},
                {"StudentID": 38, "FullName": "Lana Lang", "GPA": 3.7},
                {"StudentID": 39, "FullName": "Monica Rambeau", "GPA": 3.6},
                {"StudentID": 40, "FullName": "Nina Dobrev", "GPA": 3.4},
                {"StudentID": 41, "FullName": "Oliver Queen", "GPA": 3.3},
                {"StudentID": 42, "FullName": "Peter Parker", "GPA": 3.5},
                {"StudentID": 43, "FullName": "Quentin Lance", "GPA": 3.1},
                {"StudentID": 44, "FullName": "Raven Darkholme", "GPA": 3.2},
                {"StudentID": 45, "FullName": "Selina Kyle", "GPA": 3.9},
                {"StudentID": 46, "FullName": "Tommy Merlyn", "GPA": 3.7},
                {"StudentID": 47, "FullName": "Ursula Andress", "GPA": 3.0},
                {"StudentID": 48, "FullName": "Vanessa Fisk", "GPA": 3.4},
                {"StudentID": 49, "FullName": "Walter White", "GPA": 3.8},
                {"StudentID": 50, "FullName": "Xena Warrior", "GPA": 3.5},
            ],
            "Course": [
                {"CourseID": 1, "Year": 2020, "CourseName": "Introduction to Programming", "CourseDescription": "Learn the basics of programming using Python."},
                {"CourseID": 2, "Year": 2021, "CourseName": "Data Structures", "CourseDescription": "Study common data structures and their applications."},
                {"CourseID": 3, "Year": 2022, "CourseName": "Databases", "CourseDescription": "Explore database design, SQL, and transaction management."},
                {"CourseID": 4, "Year": 2023, "CourseName": "Machine Learning", "CourseDescription": "An introduction to machine learning concepts and algorithms."},
                {"CourseID": 5, "Year": 2020, "CourseName": "Discrete Mathematics", "CourseDescription": "Understand mathematical foundations for computer science."},
                {"CourseID": 6, "Year": 2021, "CourseName": "Operating Systems", "CourseDescription": "Learn the principles of operating systems design and implementation."},
                {"CourseID": 7, "Year": 2022, "CourseName": "Networking", "CourseDescription": "Understand the basics of computer networks and protocols."},
                {"CourseID": 8, "Year": 2023, "CourseName": "Software Engineering", "CourseDescription": "Learn software development life cycles and methodologies."},
                {"CourseID": 9, "Year": 2020, "CourseName": "Computer Graphics", "CourseDescription": "Explore the principles of 2D and 3D computer graphics."},
                {"CourseID": 10, "Year": 2021, "CourseName": "Artificial Intelligence", "CourseDescription": "Introduction to AI and its real-world applications."},
                {"CourseID": 11, "Year": 2022, "CourseName": "Cybersecurity", "CourseDescription": "Understand security principles and techniques for protecting systems."},
                {"CourseID": 12, "Year": 2023, "CourseName": "Parallel Computing", "CourseDescription": "Study parallel algorithms and multi-core processing."},
                {"CourseID": 13, "Year": 2020, "CourseName": "Algorithm Design", "CourseDescription": "Learn advanced algorithms and optimization techniques."},
                {"CourseID": 14, "Year": 2021, "CourseName": "Embedded Systems", "CourseDescription": "Explore the basics of designing embedded systems."},
                {"CourseID": 15, "Year": 2022, "CourseName": "Cloud Computing", "CourseDescription": "Understand cloud architecture and service models."},
                {"CourseID": 16, "Year": 2023, "CourseName": "Big Data Analytics", "CourseDescription": "Learn to analyze and manage large datasets."},
                {"CourseID": 17, "Year": 2020, "CourseName": "Ethical Hacking", "CourseDescription": "An introduction to ethical hacking practices and tools."},
                {"CourseID": 18, "Year": 2021, "CourseName": "Blockchain Technology", "CourseDescription": "Understand the fundamentals of blockchain systems."},
                {"CourseID": 19, "Year": 2022, "CourseName": "IoT Systems", "CourseDescription": "Learn about Internet of Things devices and networks."},
                {"CourseID": 20, "Year": 2023, "CourseName": "Natural Language Processing", "CourseDescription": "Study computational techniques for processing text data."},
                {"CourseID": 21, "Year": 2020, "CourseName": "Quantum Computing", "CourseDescription": "Explore the basics of quantum algorithms and systems."},
                {"CourseID": 22, "Year": 2021, "CourseName": "Mobile App Development", "CourseDescription": "Learn to design and build mobile applications."},
                {"CourseID": 23, "Year": 2022, "CourseName": "Game Development", "CourseDescription": "Introduction to video game design and programming."},
                {"CourseID": 24, "Year": 2023, "CourseName": "Human-Computer Interaction", "CourseDescription": "Learn principles of designing user-friendly interfaces."},
                {"CourseID": 25, "Year": 2020, "CourseName": "Robotics", "CourseDescription": "Study the principles and applications of robotics."},
                {"CourseID": 26, "Year": 2021, "CourseName": "Cryptography", "CourseDescription": "Explore encryption methods and secure communication."},
                {"CourseID": 27, "Year": 2022, "CourseName": "Compiler Design", "CourseDescription": "Learn to design and implement compilers."},
                {"CourseID": 28, "Year": 2023, "CourseName": "Virtual Reality", "CourseDescription": "Understand the basics of VR technology and development."},
                {"CourseID": 29, "Year": 2020, "CourseName": "Digital Signal Processing", "CourseDescription": "Learn techniques for processing digital signals."},
                {"CourseID": 30, "Year": 2021, "CourseName": "Data Visualization", "CourseDescription": "Explore tools and techniques for visualizing data."},
                {"CourseID": 31, "Year": 2022, "CourseName": "Web Development", "CourseDescription": "Introduction to web technologies and frameworks."},
                {"CourseID": 32, "Year": 2023, "CourseName": "Bioinformatics", "CourseDescription": "Study computational approaches to biological data."},
                {"CourseID": 33, "Year": 2020, "CourseName": "Software Testing", "CourseDescription": "Understand techniques and tools for testing software."},
                {"CourseID": 34, "Year": 2021, "CourseName": "Augmented Reality", "CourseDescription": "Explore AR technologies and development techniques."},
                {"CourseID": 35, "Year": 2022, "CourseName": "Autonomous Systems", "CourseDescription": "Learn the basics of self-driving vehicles and systems."},
                {"CourseID": 36, "Year": 2023, "CourseName": "Distributed Systems", "CourseDescription": "Understand distributed architectures and algorithms."},
                {"CourseID": 37, "Year": 2020, "CourseName": "Database Administration", "CourseDescription": "Learn to manage and optimize database systems."},
                {"CourseID": 38, "Year": 2021, "CourseName": "Advanced Machine Learning", "CourseDescription": "Explore deeper into machine learning techniques."},
                {"CourseID": 39, "Year": 2022, "CourseName": "DevOps", "CourseDescription": "Learn continuous integration and deployment practices."},
                {"CourseID": 40, "Year": 2023, "CourseName": "Data Ethics", "CourseDescription": "Understand ethical issues in data collection and use."},
                {"CourseID": 41, "Year": 2020, "CourseName": "3D Printing", "CourseDescription": "Introduction to 3D printing technologies and applications."},
                {"CourseID": 42, "Year": 2021, "CourseName": "Biomedical Engineering", "CourseDescription": "Explore engineering solutions in healthcare."},
                {"CourseID": 43, "Year": 2022, "CourseName": "Climate Informatics", "CourseDescription": "Study computational methods for climate data analysis."},
                {"CourseID": 44, "Year": 2023, "CourseName": "Social Network Analysis", "CourseDescription": "Analyze and model social network data."},
                {"CourseID": 45, "Year": 2020, "CourseName": "Sensor Networks", "CourseDescription": "Understand sensor networks and their applications."},
                {"CourseID": 46, "Year": 2021, "CourseName": "Embedded AI", "CourseDescription": "Learn about AI applications in embedded systems."},
                {"CourseID": 47, "Year": 2022, "CourseName": "Financial Computing", "CourseDescription": "Explore computational methods in finance."},
                {"CourseID": 48, "Year": 2023, "CourseName": "Edge Computing", "CourseDescription": "Understand the principles of edge computing."},
                {"CourseID": 49, "Year": 2020, "CourseName": "Functional Programming", "CourseDescription": "Learn functional programming paradigms."},
                {"CourseID": 50, "Year": 2021, "CourseName": "Microservices", "CourseDescription": "Understand the architecture of microservices."},
            ],
            "Attends": [
                {"StudentID": 1, "CourseID": 1},
                {"StudentID": 1, "CourseID": 2},
                {"StudentID": 1, "CourseID": 3},
                {"StudentID": 2, "CourseID": 1},
                {"StudentID": 2, "CourseID": 4},
                {"StudentID": 2, "CourseID": 5},
                {"StudentID": 3, "CourseID": 2},
                {"StudentID": 3, "CourseID": 6},
                {"StudentID": 3, "CourseID": 7},
                {"StudentID": 4, "CourseID": 3},
                {"StudentID": 4, "CourseID": 8},
                {"StudentID": 4, "CourseID": 9},
                {"StudentID": 5, "CourseID": 4},
                {"StudentID": 5, "CourseID": 10},
                {"StudentID": 5, "CourseID": 11},
                {"StudentID": 6, "CourseID": 5},
                {"StudentID": 6, "CourseID": 12},
                {"StudentID": 6, "CourseID": 13},
                {"StudentID": 7, "CourseID": 6},
                {"StudentID": 7, "CourseID": 14},
                {"StudentID": 7, "CourseID": 15},
                {"StudentID": 8, "CourseID": 7},
                {"StudentID": 8, "CourseID": 16},
                {"StudentID": 8, "CourseID": 17},
                {"StudentID": 9, "CourseID": 8},
                {"StudentID": 9, "CourseID": 18},
                {"StudentID": 9, "CourseID": 19},
                {"StudentID": 10, "CourseID": 9},
                {"StudentID": 10, "CourseID": 20},
                {"StudentID": 10, "CourseID": 21},
                {"StudentID": 11, "CourseID": 10},
                {"StudentID": 11, "CourseID": 22},
                {"StudentID": 11, "CourseID": 23},
                {"StudentID": 12, "CourseID": 11},
                {"StudentID": 12, "CourseID": 24},
                {"StudentID": 12, "CourseID": 25},
                {"StudentID": 13, "CourseID": 12},
                {"StudentID": 13, "CourseID": 26},
                {"StudentID": 13, "CourseID": 27},
                {"StudentID": 14, "CourseID": 13},
                {"StudentID": 14, "CourseID": 28},
                {"StudentID": 14, "CourseID": 29},
                {"StudentID": 15, "CourseID": 14},
                {"StudentID": 15, "CourseID": 30},
                {"StudentID": 15, "CourseID": 31},
                {"StudentID": 16, "CourseID": 15},
                {"StudentID": 16, "CourseID": 32},
                {"StudentID": 16, "CourseID": 33},
                {"StudentID": 17, "CourseID": 16},
                {"StudentID": 17, "CourseID": 34},
                {"StudentID": 17, "CourseID": 35},
                {"StudentID": 18, "CourseID": 17},
                {"StudentID": 18, "CourseID": 36},
                {"StudentID": 18, "CourseID": 37},
                {"StudentID": 19, "CourseID": 18},
                {"StudentID": 19, "CourseID": 38},
                {"StudentID": 19, "CourseID": 39},
                {"StudentID": 20, "CourseID": 19},
                {"StudentID": 20, "CourseID": 40},
                {"StudentID": 20, "CourseID": 41},
                {"StudentID": 21, "CourseID": 20},
                {"StudentID": 21, "CourseID": 42},
                {"StudentID": 21, "CourseID": 43},
                {"StudentID": 22, "CourseID": 21},
                {"StudentID": 22, "CourseID": 44},
                {"StudentID": 22, "CourseID": 45},
                {"StudentID": 23, "CourseID": 22},
                {"StudentID": 23, "CourseID": 46},
                {"StudentID": 23, "CourseID": 47},
                {"StudentID": 24, "CourseID": 23},
                {"StudentID": 24, "CourseID": 48},
                {"StudentID": 24, "CourseID": 49},
                {"StudentID": 25, "CourseID": 24},
                {"StudentID": 25, "CourseID": 50},
                {"StudentID": 26, "CourseID": 25},
                {"StudentID": 26, "CourseID": 1},
                {"StudentID": 26, "CourseID": 2},
                {"StudentID": 27, "CourseID": 26},
                {"StudentID": 27, "CourseID": 3},
                {"StudentID": 27, "CourseID": 4},
                {"StudentID": 28, "CourseID": 27},
                {"StudentID": 28, "CourseID": 5},
                {"StudentID": 28, "CourseID": 6},
                {"StudentID": 29, "CourseID": 28},
                {"StudentID": 29, "CourseID": 7},
                {"StudentID": 29, "CourseID": 8},
                {"StudentID": 30, "CourseID": 29},
                {"StudentID": 30, "CourseID": 9},
                {"StudentID": 30, "CourseID": 10},
                {"StudentID": 31, "CourseID": 11},
                {"StudentID": 31, "CourseID": 12},
                {"StudentID": 31, "CourseID": 13},
                {"StudentID": 32, "CourseID": 14},
                {"StudentID": 32, "CourseID": 15},
                {"StudentID": 32, "CourseID": 16},
                {"StudentID": 33, "CourseID": 17},
                {"StudentID": 33, "CourseID": 18},
                {"StudentID": 33, "CourseID": 19},
                {"StudentID": 34, "CourseID": 20},
                {"StudentID": 34, "CourseID": 21},
                {"StudentID": 34, "CourseID": 22},
                {"StudentID": 35, "CourseID": 23},
                {"StudentID": 35, "CourseID": 24},
                {"StudentID": 35, "CourseID": 25},
                {"StudentID": 36, "CourseID": 26},
                {"StudentID": 36, "CourseID": 27},
                {"StudentID": 36, "CourseID": 28},
                {"StudentID": 37, "CourseID": 29},
                {"StudentID": 37, "CourseID": 30},
                {"StudentID": 37, "CourseID": 31},
                {"StudentID": 38, "CourseID": 32},
                {"StudentID": 38, "CourseID": 33},
                {"StudentID": 38, "CourseID": 34},
                {"StudentID": 39, "CourseID": 35},
                {"StudentID": 39, "CourseID": 36},
                {"StudentID": 39, "CourseID": 37},
                {"StudentID": 40, "CourseID": 38},
                {"StudentID": 40, "CourseID": 39},
                {"StudentID": 40, "CourseID": 40},
                {"StudentID": 41, "CourseID": 41},
                {"StudentID": 41, "CourseID": 42},
                {"StudentID": 41, "CourseID": 43},
                {"StudentID": 42, "CourseID": 44},
                {"StudentID": 42, "CourseID": 45},
                {"StudentID": 42, "CourseID": 46},
                {"StudentID": 43, "CourseID": 47},
                {"StudentID": 43, "CourseID": 48},
                {"StudentID": 43, "CourseID": 49},
                {"StudentID": 44, "CourseID": 50},
                {"StudentID": 44, "CourseID": 1},
                {"StudentID": 44, "CourseID": 2},
                {"StudentID": 45, "CourseID": 3},
                {"StudentID": 45, "CourseID": 4},
                {"StudentID": 45, "CourseID": 5},
                {"StudentID": 46, "CourseID": 6},
                {"StudentID": 46, "CourseID": 7},
                {"StudentID": 46, "CourseID": 8},
                {"StudentID": 47, "CourseID": 9},
                {"StudentID": 47, "CourseID": 10},
                {"StudentID": 47, "CourseID": 11},
                {"StudentID": 48, "CourseID": 12},
                {"StudentID": 48, "CourseID": 13},
                {"StudentID": 48, "CourseID": 14},
                {"StudentID": 49, "CourseID": 15},
                {"StudentID": 49, "CourseID": 16},
                {"StudentID": 49, "CourseID": 17},
                {"StudentID": 50, "CourseID": 18},
                {"StudentID": 50, "CourseID": 19},
                {"StudentID": 50, "CourseID": 20},
            ],
        }

    def execute_query(self, query_string: str) -> ExecutionResult:
        query_tree: ParseTree = get_parse_tree(query_string)
        transaction_id = 0
        timestamp = datetime.datetime.now()
        message = "Query is successful"
        data = self.check_first_child(query_tree)
        return ExecutionResult(transaction_id, timestamp, message, data, query_string)
        # query_plan belum jadi, sementara proses query tree dulu, lagipula nanti query plan juga bentuknya objek query tree, tapi teroptimasi

    # cek child pertama dari parse tree untuk menentukan jenis query SQL (SELECT, UPDATE, atau TRANSACTION)
    def check_first_child(self, query_tree: ParseTree) -> Rows:
        # child pertama adalah elemen pertama pada atribut childs dari ParseTree, berupa list of ParseTree
        first_child_node = query_tree.childs[0]
        # jika child pertama merupakan token SELECT
        if (isinstance(first_child_node.root, Node)) and (first_child_node.root.token_type in {Token.SELECT}):
            # panggil fungsi execute_SELECT
            return (self.execute_SELECT(query_tree))

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
    def add_to_dictionary(self, dictionary: Union[Dict[str, List[str]] | Dict[str, List[Condition]]], key: str, value: Union[str | Condition]):
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

    # handle query SELECT, menerima input node query tree
    def execute_SELECT(self, query_tree: ParseTree) -> Rows:
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
        # dictionary untuk menyimpan kondisi setiap tabel
        conditions: Dict[str, List[Condition]] = {}
        # tambahkan nama tabel ke dalam skema dari node FromList
        self.execute_FROM(retrieval_schema, alias, query_tree)
        # Testing (DELETE LATER)
        print("schema =", retrieval_schema, "\n")
        print("alias =", alias, "\n")
        # jika ada klausa berikutnya
        if len(query_tree.childs) >= 6:
            # jika ada klausa WHERE
            if query_tree.childs[5].root == "Condition":
                self.execute_WHERE(retrieval_schema, alias, conditions, query_tree)
                for table, conditions in conditions.items():
                    print(f"Table: {table}")
                    for condition in conditions:
                        print(f"  - {condition.column} {condition.operation} {condition.operand}")
        # Mengambil dari StorageManager
        rows = []
        for table, attributes in retrieval_schema.items() :
            print(table)
            print(attributes)
            
            data_retrieval = DataRetrieval(table, attributes, conditions, None, None)
            rows.append(self.storage_manager.read_block(data_retrieval))
        
        # jika ada klausa berikutnya
        if len(query_tree.childs) >= 6:
            # jika ada klausa LIMIT
            if query_tree.childs[4].root.value == "LIMIT":
                limit_value = float(query_tree.childs[5].root.value)
                limit_value = int(limit_value)
                rows = [row[:limit_value] for row in rows]
            elif query_tree.childs[6].root.value == "LIMIT":
                limit_value = float(query_tree.childs[7].root.value)
                limit_value = int(limit_value)
                rows = [row[:limit_value] for row in rows]

        order_by = str(input("Mau menggunakan klausa ORDER BY? (Y/N) "))
        while (order_by != "Y" and order_by != "N"):
            print("Pilihan tidak valid")
            order_by = str(input("Mau menggunakan klausa ORDER BY? (Y/N) "))
        if order_by == "Y":
            order_by_attr = str(input("Masukkan nama atribut acuan: "))
            ordering = int(input("Skema pengurutan (1 untuk ASC, 2 UNTUK DESC): "))
            if ordering == 2:
                descending = True
            else:
                descending = False
            rows = [item for sublist in rows for item in sublist]
            self.execute_ORDER_BY(rows, order_by_attr, descending)
        
        return Rows(rows)

    def add_condition_from_AndConditionTail(self, schema: Dict[str, List[str]], alias: Dict[str, str], conditions: Dict[str, List[Condition]], AndConditionTail_node: ParseTree):
        ConditionTerm_node = AndConditionTail_node.childs[1]
        Field_node = ConditionTerm_node.childs[0]
        # jika tidak menggunakan alias
        if len(Field_node.childs) == 1:
            table = list(schema.keys())[0]
            column = Field_node.childs[0].root.value
        # jika menggunakan alias
        else:
            table_alias = Field_node.childs[0].root.value
            table = alias[table_alias]
            column = Field_node.childs[2].root.value
        operation = ConditionTerm_node.childs[1].childs[0].root.value
        operand = ConditionTerm_node.childs[2].root.value
        # tambahkan kondisi ke dictionary
        self.add_to_dictionary(conditions, table, Condition(column, operation, operand))
        # jika ada kondisi lain
        if len(AndConditionTail_node.childs) == 3:
            AndConditionTail_node = AndConditionTail_node.childs[2]
            self.add_condition_from_AndConditionTail(schema, alias, conditions, AndConditionTail_node)

    def execute_WHERE(self, schema: Dict[str, List[str]], alias: Dict[str, str], conditions: Dict[str, List[Condition]], query_tree: ParseTree) -> Dict[str, List[Condition]]:
        Condition_node = query_tree.childs[5]
        AndCondition_node = Condition_node.childs[0]
        ConditionTerm_node = AndCondition_node.childs[0]
        Field_node = ConditionTerm_node.childs[0]
        # jika tidak menggunakan alias
        if len(Field_node.childs) == 1:
            table = list(schema.keys())[0]
            column = Field_node.childs[0].root.value
        # jika menggunakan alias
        else:
            table_alias = Field_node.childs[0].root.value
            table = alias[table_alias]
            column = Field_node.childs[2].root.value
        operation = ConditionTerm_node.childs[1].childs[0].root.value
        operand = ConditionTerm_node.childs[2].root.value
        # tambahkan kondisi ke dictionary
        self.add_to_dictionary(conditions, table, Condition(column, operation, operand))
        # jika ada kondisi lain
        if len(AndCondition_node.childs) == 2:
            AndConditionTail_node = AndCondition_node.childs[1]
            self.add_condition_from_AndConditionTail(schema, alias, conditions, AndConditionTail_node)
        return conditions

    # fungsi untuk ORDER BY
    def execute_ORDER_BY(self, rows: List[Dict[str, Union[int, str]]], order_by_attr: str, descending: bool = False) -> List[Dict[str, Union[int, str]]]:
        def get_sort_key(row):
            value = row.get(order_by_attr, None)
            if isinstance(value, str):
                return tuple(ord(char) for char in value)
            return value
        return sorted(rows, key=get_sort_key, reverse=descending)

    # TODO (menunggu kelompok query optimizer)
    # handle query UPDATE, menerima input node query tree
    def execute_UPDATE(self, query_tree: ParseTree):
        pass

    # TODO (menunggu kelompok query optimizer)
    # handle query BEGIN TRANSACTION, menerima input query tree
    def execute_BEGIN_TRANSCATION(self, query_tree: ParseTree):
        pass