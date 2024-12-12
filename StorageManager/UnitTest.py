import unittest
import os
import shutil
from StorageManager.classes import StorageManager, DataWrite, DataRetrieval, DataDeletion, Condition, ConditionGroup
from StorageManager.HashIndex import Hash

class TestStorageManager(unittest.TestCase):
    def setUp(self):
        self.manager = StorageManager()
        self.manager.data = {
            "Student": [
                {"StudentID": 1, "FullName": "Alice", "GPA": 3.5},
                {"StudentID": 2, "FullName": "Bob", "GPA": 3.8},
            ]
        }

    def test_read_block(self):
        conditions = [Condition("GPA", ">", 3.6)]
        data_retrieval = DataRetrieval("Student", ["FullName"], conditions)
        result = self.manager.read_block(data_retrieval)
        self.assertEqual(result, [{"FullName": "Bob"}])

    def test_write_block(self):
        data_write = DataWrite("Student", ["GPA"], [4.0], [Condition("StudentID", "=", 1)])
        affected = self.manager.write_block(data_write)
        self.assertEqual(affected, 1)
        self.assertEqual(self.manager.data["Student"][0]["GPA"], 4.0)

    def test_delete_block(self):
        data_deletion = DataDeletion("Student", [Condition("GPA", "<", 3.6)])
        removed = self.manager.delete_block(data_deletion)
        self.assertEqual(removed, 1)
        self.assertEqual(len(self.manager.data["Student"]), 1)
    
    def test_write_block_with_logging(self):
        data_write = DataWrite("Student", ["GPA"], [4.0], [Condition("StudentID", "=", 1)])
        self.manager.write_block(data_write)
        last_log = self.manager.logs[-1]
        self.assertEqual(last_log["action"], "write")
        self.assertIn("old_new_values", last_log["details"])
        self.assertEqual(last_log["details"]["old_new_values"][0]["old"]["GPA"], 3.5)
        self.assertEqual(last_log["details"]["old_new_values"][0]["new"]["GPA"], 4.0)

class TestHashIndex(unittest.TestCase):
    def setUp(self):
        self.test_data_dir = "test_data_blocks/"
        self.test_hash_dir = "test_hash/"
        
        os.makedirs(self.test_data_dir, exist_ok=True)
        os.makedirs(os.path.join(self.test_data_dir, self.test_hash_dir), exist_ok=True)
        
        Hash.change_config(DATA_DIR=self.test_data_dir, HASH_DIR=self.test_hash_dir)
        
        self.manager = StorageManager()
        
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
        
        self.manager.write_block_to_disk(write_student)
        self.manager.write_block_to_disk(write_student2)

    def tearDown(self):
        shutil.rmtree(self.test_data_dir)

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
        
        self.manager.write_block_to_disk(write_student3)
        
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
        
        deleted_count = self.manager.delete_block_to_disk(delete_student)
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