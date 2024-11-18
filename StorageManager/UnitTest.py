import unittest
from classes import StorageManager, DataRetrieval, DataWrite, DataDeletion, Condition

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

if __name__ == "__main__":
    unittest.main()