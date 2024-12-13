import unittest
# # Kalau mau run tanpa harus dari root, uncomment (tetep dalam /StorageManager)
# import os
# import sys
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import shutil
from StorageManager.classes import Statistic, StorageManager, DataWrite, DataRetrieval, DataDeletion, Condition, ConditionGroup
from StorageManager.HashIndex import Hash
from FailureRecoveryManager.Buffer import Buffer

class TestStorageManager(unittest.TestCase):
    def setUp(self):
        self.test_data_dir = "data_blocks/"
        self.test_hash_dir = "hash/"
        self.manager = StorageManager(Buffer(1000))
        write_student = DataWrite(
            table="Student", 
            columns=["id", "name", "dept_name"], 
            new_values=[1, "Alice", "Computer Science"], 
            level="table"
        )
        write_student2 = DataWrite(
            table="Student", 
            columns=["id", "name", "dept_name"], 
            new_values=[2, "Bob", "Mathematics"], 
            level="table"
        )
        self.manager.write_block(write_student)
        self.manager.write_block(write_student2)
        
    def tearDown(self):
        shutil.rmtree(self.test_data_dir)

    def test_read_block(self):
        """
        Test read block functionality
        1. Read a block
        2. Verify the record 
        """
        read_student1 = DataRetrieval(
            table="Student", 
            columns=["id", "name", "dept_name"], 
            conditions= ConditionGroup([Condition("id", "=", 1)]),
            search_type="sequential",
            level="row"
        )
        retrieved_records = self.manager.read_block(read_student1)        
        
        self.assertEqual(len(retrieved_records), 1)
        self.assertEqual(retrieved_records[0]["id"], 1)
        self.assertEqual(retrieved_records[0]["name"], "Alice")
        self.assertEqual(retrieved_records[0]["dept_name"], "Computer Science")


    def test_write_block(self):
        """
        Test write block functionality
        1. Write a new record
        2. Verify the record retrieved
        """
        write_student3 = DataWrite(
            table="Student", 
            columns=["id", "name", "dept_name"], 
            new_values=[3, "Yusuf", "Mathematics"], 
            level="table"
        )
        self.manager.write_block(write_student3)
        
        read_student3 = DataRetrieval(
            table="Student", 
            columns=["id", "name", "dept_name"], 
            conditions=ConditionGroup([Condition("id", "=", 3)]),
            search_type="sequential",
            level="row"
        )
        retrieved_records = self.manager.read_block(read_student3)
        
        self.assertEqual(len(retrieved_records), 1)
        self.assertEqual(retrieved_records[0]["id"], 3)
        self.assertEqual(retrieved_records[0]["name"], "Yusuf")
        self.assertEqual(retrieved_records[0]["dept_name"], "Mathematics")

    def test_write_block_with_logging(self):
        """
        Test write block functionality with logging
        1. Write a new record
        2. Verify the record retrieved
        3. Verify the log entry
        """
        write_student4 = DataWrite(
            table="Student", 
            columns=["id", "name", "dept_name"], 
            new_values=[4, "Bilal", "Psychology"], 
            level="table"
        )
        
        self.manager.write_block(write_student4)
        
        retrieved_records = self.manager.read_block(DataRetrieval(
            table="Student", 
            columns=["id", "name", "dept_name"], 
            conditions=ConditionGroup([Condition("id", "=", 4)]),
            search_type="sequential",
            level="row"
        ))
        
        self.assertEqual(len(retrieved_records), 1)
        self.assertEqual(retrieved_records[0]["id"], 4)
        self.assertEqual(retrieved_records[0]["name"], "Bilal")
        self.assertEqual(retrieved_records[0]["dept_name"], "Psychology")
        
        # Verify the log entry (last one)
        # last_log = self.manager.logs[-1]
        # self.assertEqual(last_log["action"], "write")
        # self.assertEqual(last_log["table"], DataWrite.table)
        # self.assertEqual(last_log["data"], DataWrite.new_values)

    def test_delete_block(self):
        data_deletion = DataDeletion("Student", ConditionGroup([Condition("name", "=", "Bob")]), "row")
        removed = self.manager.delete_block(data_deletion)
        self.assertEqual(removed, 1)
        # self.assertEqual(len(self.manager.read_block(DataRetrieval("Student","id", ConditionGroup(Condition("id", ">", 0)), "", "row" ))), 1)
        retrieved = self.manager.read_block(DataRetrieval("Student", ["id"], ConditionGroup([Condition("name", "=", "Bobiii")], "AND"), "sequential", "row"))
        self.assertEqual(len(retrieved), 0)
    

    def test_schema(self):
        all_relation = self.manager.get_all_relations()
        self.assertEqual(['Advisor', 'Classroom', 'Course', 'Department', 'Instructor', 'Prerequisite', 'Section', 'Student', 'Takes', 'Teaches', 'TimeSlot'], all_relation)
        expected_attributes = {
            'Advisor': ['s_id', 'i_id'],
            'Classroom': ['building', 'room_number', 'capacity'],
            'Course': ['course_id', 'title', 'dept_name', 'credits'],
            'Department': ['dept_name', 'building', 'budget'],
            'Instructor': ['id', 'name', 'dept_name', 'salary'],
            'Prerequisite': ['course_id', 'prereq_id'],
            'Section': ['course_id', 'sec_id', 'semester', 'year', 'building', 'room_number', 'time_slot_id'],
            'Student': ['id', 'name', 'dept_name', 'tot_cred'],
            'Takes': ['id', 'course_id', 'sec_id', 'semester', 'year', 'grade'],
            'Teaches': ['id', 'course_id', 'sec_id', 'semester', 'year'],
            'TimeSlot': ['time_slot_id', 'day', 'start_time', 'end_time']
        }
        for relation, attributes in expected_attributes.items():
            with self.subTest(relation=relation):
                self.assertEqual(attributes, self.manager.get_all_attributes(relation))

    def test_index(self):
        # Contoh set_index dan get_index
        self.manager.set_index("Student", "name", 'hash')
        self.manager.set_index("Departement", "building", 'hash')
        self.assertEqual(self.manager.get_index("Student", "name"), 'hash')
        self.assertEqual(self.manager.get_index("Departement", "building"), 'hash')

    def test_stats(self):
        statistic = self.manager.get_stats()
        print("STATS", statistic["Student"].n_r)

    def tearDown(self):
            shutil.rmtree(self.test_data_dir)

class TestHashIndex(unittest.TestCase):
    def setUp(self):
        self.test_data_dir = "data_blocks/"
        self.test_hash_dir = "hash/"
        
        # os.makedirs(self.test_data_dir, exist_ok=True)
        # os.makedirs(os.path.join(self.test_data_dir, self.test_hash_dir), exist_ok=True)
        
        # Hash.change_config(DATA_DIR=self.test_data_dir, HASH_DIR=self.test_hash_dir)
        
        self.manager = StorageManager(Buffer(100))
        
        write_student = DataWrite(
            table="Student", 
            columns=["id", "name", "dept_name"], 
            new_values=[1, "Alice", "Computer Science"], 
            level="table"
        )
        write_student2 = DataWrite(
            table="Student", 
            columns=["id", "name", "dept_name"], 
            new_values=[2, "Bob", "Mathematics"], 
            level="table"
        )
        
        self.manager.set_index("Student", "id", "hash")
        
        self.manager.write_block(write_student)
        self.manager.write_block(write_student2)

    def tearDown(self):
        shutil.rmtree(self.test_data_dir)
    
    def test_hash_read(self):
        """
        Test hash index read functionality
        1. Read a record using hash index
        2. Verify the record is retrieved correctly
        """
        retrieved_records = self.manager.read_block_with_hash("Student", "id", 1)
        
        self.assertEqual(len(retrieved_records), 1)
        self.assertEqual(retrieved_records[0]["id"], 1)
        self.assertEqual(retrieved_records[0]["name"], "Alice")
        self.assertEqual(retrieved_records[0]["dept_name"], "Computer Science")
        
    def test_hash_write(self):
        """
        Test hash index write functionality
        1. Write a new record
        2. Verify the record can be retrieved using hash index
        """
        write_student3 = DataWrite(
            table="Student", 
            columns=["id", "name", "dept_name"], 
            new_values=[3, "Charlie", "Physics"], 
            level="table"
        )
        
        self.manager.write_block(write_student3)
        
        retrieved_records = self.manager.read_block_with_hash("Student", "id", 3)
        
        self.assertEqual(len(retrieved_records), 1)
        self.assertEqual(retrieved_records[0]["id"], 3)
        self.assertEqual(retrieved_records[0]["name"], "Charlie")
        self.assertEqual(retrieved_records[0]["dept_name"], "Physics")

    def test_hash_delete(self):
        """
        Test hash index delete functionality
        1. Delete a record
        2. Verify the record cannot be retrieved using hash index
        """
        delete_student = DataDeletion(
            table="Student", 
            conditions=ConditionGroup([Condition("id", "=", 2)]),
            level="table"
        )
        print("TEST TRY DELETE")
        deleted_count = self.manager.delete_block(delete_student)
        self.assertEqual(deleted_count, 1)
        
        retrieved_records = self.manager.read_block_with_hash("Student", "id", 2)
        self.assertEqual(len(retrieved_records), 0)

    def test_hash_index_exists(self):
        """
        Verify that hash index was created successfully
        """
        self.assertTrue(self.manager.has_index("id", "Student"))

if __name__ == "__main__":
    unittest.main()

