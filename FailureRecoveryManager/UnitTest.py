import datetime
import unittest
from unittest.mock import MagicMock, patch
import json

# from FailureRecoveryManager.Rows import Rows
from FailureRecoveryManager.RecoverCriteria import RecoverCriteria
from FailureRecoveryManager.FailureRecoveryManager import FailureRecoveryManager
from FailureRecoveryManager.LRUCache import LRUCache
from FailureRecoveryManager.Buffer import Buffer
from StorageManager.classes import StorageManager, DataWrite, DataDeletion, Condition
from ConcurrencyControlManager.classes import TransactionAction, Table, Row, Cell, PrimaryKey


class TestFailureRecoveryManager(unittest.TestCase):
    # Go to root directory (\Tubes-SBD-MarioDB>)
    # Run python -m unittest -v  "FailureRecoveryManager.UnitTest"
    def setUp(self):
        self.lru = LRUCache(5)
        self.buffer = Buffer(5)
        self.frm = FailureRecoveryManager(buffer=self.buffer,log_file="./FailureRecoveryManager/log2.log")
        

    def test_lru_cache(self):
        """
        Method for testing the LRU Cache class
        """
        # Test case 1: Inserting a new value into the cache
        self.lru.put("1", "Alice")
        self.assertEqual(self.lru.get("1"), "Alice")
        self.lru.put("2", "Bob")
        self.assertEqual(self.lru.get("2"), "Bob")
        self.lru.put("3", "Charlie")
        self.assertEqual(self.lru.get("3"), "Charlie")
        self.lru.put("4", "David")
        self.assertEqual(self.lru.get("4"), "David")
        self.lru.put("5", "Eve")
        self.assertEqual(self.lru.get("5"), "Eve")

        # Test case 2: Inserting a value into the cache that exceeds the cache size
        self.lru.put("6", "Frank")
        self.assertEqual(self.lru.get("6"), "Frank")
        self.assertEqual(self.lru.get("1"), None)

        self.lru.put("7", "Grace")
        self.assertEqual(self.lru.get("7"), "Grace")
        self.assertEqual(self.lru.get("2"), None)

        # Test case 3: deleting a value from the cache
        self.lru.delete("3")
        self.assertEqual(self.lru.get("3"), None)

        self.lru.delete("4")
        self.assertEqual(self.lru.get("4"), None)

        # Test case 4: Inserting a value into the cache that already exists
        self.lru.put("5", "Paul")
        self.assertEqual(self.lru.get("5"), "Paul")

        # Test case 5: Accessing a value that does not exist in the cache
        self.assertEqual(self.lru.get("-1"), None)
        self.assertEqual(self.lru.get("69"), None)

        # Test case 6: clearing the cache
        self.lru.clear()
        for i in range(1, 8):
            self.assertEqual(self.lru.get(str(i)), None)

    def test_write_log(self):
        self.frm._wa_logs = [
            '102|WRITE|employees|None|[{"id": 2, "name": "Bob", "salary": 4000}]',
            '102|WRITE|employees|[{"id": 2, "name": "Bob", "salary": 4000}]|None',
            '102|ABORT',
            '103|WRITE|employees|[{"id": 1, "name": "Alice", "salary": 5000}]|[{"id": 1, "name": "Alice", "salary": 6000}]',
        ]
        # execution_result = ExecutionResult(
        #     transaction_id=102,
        #     data_before=Rows(None),
        #     data_after=Rows([{"id": 2, "name": "Bob", "salary": 4000}]),
        #     status="WRITE",
        #     table="employees",
        # )
        table = Table("employees")
            
        before_states = Row(table, PrimaryKey(None), None)
        after_states = Row(table, PrimaryKey(None), {"id": 2, "name": "Bob", "salary": 4000})

        transaction_action = TransactionAction(102,"WRITE", "row", after_states, before_states)

        self.frm.write_log(transaction_action)
        self.assertEqual(int((self.frm._wa_logs[4]).split("|")[0]), 102)
        self.assertEqual(self.frm._wa_logs[4].split("|")[1], "WRITE")
        self.assertEqual(self.frm._wa_logs[4].split("|")[2], "employees")
        self.assertEqual(self.frm._wa_logs[4].split("|")[3], "None")
        self.assertEqual(
            self.frm._wa_logs[4].split("|")[4],
            json.dumps([{"id": 2, "name": "Bob", "salary": 4000}]),
        )


    def test_is_wa_log_full_no_spare(self):
        self.frm._wa_logs = ["log"] * self.frm._max_size_log
        self.assertTrue(self.frm.is_wa_log_full())

    def test_save_checkpoint(self):
        """Test to ensure checkpoint can be saved and contains correct log entries"""
        self.frm._wa_logs.append("102|COMMIT")

        table = Table("employees")
            
        before_states = Row(table, PrimaryKey(None), None)
        after_states = Row(table, PrimaryKey(None), {"id": 2, "name": "Bob", "salary": 4000})

        transaction_action_1 = TransactionAction(102,"WRITE", "row", after_states, before_states)
        transaction_action_2 = TransactionAction(102,"ABORT", "row", None, None)

        self.frm.write_log(transaction_action_1)
        self.frm.write_log(transaction_action_2)
        self.frm._save_checkpoint()
        self.assertEqual(self.frm._wa_logs, [])

    # test_recover 
    @patch.object(FailureRecoveryManager, "_read_lines_from_end", return_value=[
            'CHECKPOINT|[]',
            '101|START',
            '101|WRITE|employees|None|[{"id": 1, "name": "Alice", "salary": 5000}]',
            '101|COMMIT',
            '102|START',
            '103|START',
            '103|WRITE|employees|[{"id": 2, "name": "John", "salary": 10100}]|[{"id": 2, "name": "John B", "salary": 8000}]',
            'CHECKPOINT|[102,103]',
        ][::-1])
    @patch.object(FailureRecoveryManager, "_start_checkpoint_cron_job")
    @patch("StorageManager.classes.StorageManager")
    def test_recover(self, mock_storage_manager, mock_start_checkpoint_cron_job, mock_read_lines_from_end):
        with open("./FailureRecoveryManager/log3.log", 'w') as file:
            pass
        mock_start_checkpoint_cron_job.return_value = None
        mock_storage = mock_storage_manager
        mock_storage.write_block = MagicMock(
            side_effect=lambda data_write: (
                print(
                    "\n=========WRITE_BLOCK================",
                    f"\ntable: {data_write.table}",
                    f"\ncolumn: {data_write.columns}",
                    f"\nValue: {data_write.new_values}",
                    f"\nCondition: {list(map(lambda v: v.__dict__, data_write.conditions))}",
                    "\n====================================\n"
                ),
                1  
            )[1] 
        )

        mock_storage.delete_block = MagicMock(
            side_effect=lambda data_delete: (
                print(
                    "\n=========DELETE_BLOCK================",
                    f"\ntable: {data_delete.table}",
                    f"\nCondition: {list(map(lambda v: v.__dict__, data_delete.conditions))}",
                    "\n====================================\n"
                ),
                1  
            )[1] 
        )

        
        
        frm = FailureRecoveryManager(buffer=Buffer(5), log_file="./FailureRecoveryManager/log3.log", storage_manager=mock_storage)
        frm._wa_logs = [
            '102|WRITE|employees|None|[{"id": 2, "name": "Bob", "salary": 4000}]',
            '102|WRITE|employees|[{"id": 2, "name": "Bob", "salary": 4000}]|None',
            '102|ABORT',
            '103|WRITE|employees|[{"id": 1, "name": "Alice", "salary": 5000}]|[{"id": 1, "name": "Alice", "salary": 6000}]',
        ]

        criteria = RecoverCriteria(transaction_id=103)

        frm.recover(criteria)
        
        print("wa_logs_last: ",frm._wa_logs)
        
        
        
        self.assertEqual(len(frm._wa_logs), 7)
        mock_read_lines_from_end.assert_called_once()
        self.assertEqual(mock_storage.write_block.call_count, 2)
        
        
        expected_logs = [
            '102|WRITE|employees|None|[{"id": 2, "name": "Bob", "salary": 4000}]',
            '102|WRITE|employees|[{"id": 2, "name": "Bob", "salary": 4000}]|None',
            '102|ABORT',
            '103|WRITE|employees|[{"id": 1, "name": "Alice", "salary": 5000}]|[{"id": 1, "name": "Alice", "salary": 6000}]',
            '103|WRITE|employees|[{"id": 1, "name": "Alice", "salary": 6000}]|[{"id": 1, "name": "Alice", "salary": 5000}]',
            '103|WRITE|employees|[{"id": 2, "name": "John B", "salary": 8000}]|[{"id": 2, "name": "John", "salary": 10100}]',
            '103|ABORT',
        ]
        
        self.assertEqual(frm._wa_logs, expected_logs)
        
        with open("./FailureRecoveryManager/log3.log", "r") as f:
            actual_content = f.read()
            
        self.assertEqual(actual_content.strip(), "\n".join(expected_logs))
        with open("./FailureRecoveryManager/log3.log", 'w') as file:
            pass

    
        
        
    
    # recover_system_crash
    @patch.object(FailureRecoveryManager, "_start_checkpoint_cron_job")
    @patch("StorageManager.classes.StorageManager")
    def test_recover_system_crash(self, mock_storage_manager, mock_start_checkpoint_cron_job):
        log_data = [
            'CHECKPOINT|[]',
            '101|START',
            '101|WRITE|employees|None|[{"id": 1, "name": "Alice", "salary": 5000}]',
            '101|COMMIT',
            '102|START',
            '103|START',
            '103|WRITE|employees|[{"id": 2, "name": "John", "salary": 10100}]|[{"id": 2, "name": "John B", "salary": 8000}]',
            'CHECKPOINT|[102,103]',
            '102|WRITE|employees|None|[{"id": 2, "name": "Bob", "salary": 4000}]',
            '102|WRITE|employees|[{"id": 2, "name": "Bob", "salary": 4000}]|None',
            '102|ABORT',
            '103|WRITE|employees|[{"id": 1, "name": "Alice", "salary": 5000}]|[{"id": 1, "name": "Alice", "salary": 6000}]',
        ]

        with open("./FailureRecoveryManager/log4.log", "w") as log_file:
            log_file.write("\n".join(log_data))
            log_file.write("\n")
        mock_start_checkpoint_cron_job.return_value = None
        mock_storage = mock_storage_manager
        mock_storage.write_block = MagicMock(
            side_effect=lambda data_write: (
                print(
                    "\n=========WRITE_BLOCK================",
                    f"\ntable: {data_write.table}",
                    f"\ncolumn: {data_write.columns}",
                    f"\nValue: {data_write.new_values}",
                    f"\nCondition: {list(map(lambda v: v.__dict__, data_write.conditions))}",
                    "\n====================================\n"
                ),
                1  
            )[1] 
        )

        mock_storage.delete_block = MagicMock(
            side_effect=lambda data_delete: (
                print(
                    "\n=========DELETE_BLOCK================",
                    f"\ntable: {data_delete.table}",
                    f"\nCondition: {list(map(lambda v: v.__dict__, data_delete.conditions))}",
                    "\n====================================\n"
                ),
                1  
            )[1] 
        )

        
        
        frm = FailureRecoveryManager(buffer=Buffer(5), log_file="./FailureRecoveryManager/log4.log", storage_manager=mock_storage)
        
        frm.recover_system_crash()
        
        self.assertEqual(len(frm._wa_logs), 3)
        self.assertEqual(mock_storage.write_block.call_count, 4)
        self.assertEqual(mock_storage.delete_block.call_count, 1)
        
        
        expected_logs = [
            '103|WRITE|employees|[{"id": 1, "name": "Alice", "salary": 6000}]|[{"id": 1, "name": "Alice", "salary": 5000}]',
            '103|WRITE|employees|[{"id": 2, "name": "John B", "salary": 8000}]|[{"id": 2, "name": "John", "salary": 10100}]',
            '103|ABORT',
        ]
        
        self.assertEqual(frm._wa_logs, expected_logs)
        
        with open("./FailureRecoveryManager/log4.log", "r") as f:
            actual_content = f.read()
            
            
        self.assertEqual(actual_content.strip(), "\n".join(log_data + expected_logs))
        with open("./FailureRecoveryManager/log4.log", 'w') as file:
            pass


if __name__ == "__main__":
    unittest.main()
