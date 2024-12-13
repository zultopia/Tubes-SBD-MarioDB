import json
import unittest
from unittest.mock import MagicMock, patch

from ConcurrencyControlManager.utils import PrimaryKey, Row, Table, TransactionAction
from FailureRecoveryManager.Buffer import Buffer
from FailureRecoveryManager.FailureRecoveryManager import FailureRecoveryManager
from FailureRecoveryManager.LRUCache import LRUCache

# from FailureRecoveryManager.Rows import Rows
from FailureRecoveryManager.RecoverCriteria import RecoverCriteria


class TestFailureRecoveryManager(unittest.TestCase):
    # Go to root directory (\Tubes-SBD-MarioDB>)
    # Run python -m unittest -v  "FailureRecoveryManager.UnitTest"
    def setUp(self):
        pass

    def test_lru_cache(self):
        """
        Method for testing the LRU Cache class
        """
        lru = LRUCache(5)
        # Test case 1: Inserting a new value into the cache
        lru.put("1", "Alice")
        self.assertEqual(lru.get("1"), "Alice")
        lru.put("2", "Bob")
        self.assertEqual(lru.get("2"), "Bob")
        lru.put("3", "Charlie")
        self.assertEqual(lru.get("3"), "Charlie")
        lru.put("4", "David")
        self.assertEqual(lru.get("4"), "David")
        lru.put("5", "Eve")
        self.assertEqual(lru.get("5"), "Eve")

        # Test case 2: Inserting a value into the cache that exceeds the cache size
        lru.put("6", "Frank")
        self.assertEqual(lru.get("6"), "Frank")
        self.assertEqual(lru.get("1"), None)

        lru.put("7", "Grace")
        self.assertEqual(lru.get("7"), "Grace")
        self.assertEqual(lru.get("2"), None)

        # Test case 3: deleting a value from the cache
        lru.delete("3")
        self.assertEqual(lru.get("3"), None)

        lru.delete("4")
        self.assertEqual(lru.get("4"), None)

        # Test case 4: Inserting a value into the cache that already exists
        lru.put("5", "Paul")
        self.assertEqual(lru.get("5"), "Paul")

        # Test case 5: Accessing a value that does not exist in the cache
        self.assertEqual(lru.get("-1"), None)
        self.assertEqual(lru.get("69"), None)

        # Test case 6: clearing the cache
        lru.clear()
        for i in range(1, 8):
            self.assertEqual(lru.get(str(i)), None)

    @patch.object(FailureRecoveryManager, "_start_checkpoint_cron_job")
    def test_write_log(self, _):
        with open("./FailureRecoveryManager/test/log_write.log", "w") as file:
            pass

        frm = FailureRecoveryManager(
            buffer=Buffer(5), log_file="./FailureRecoveryManager/test/log_write.log"
        )
        frm._wa_logs = [
            '102|WRITE|employees|None|[{"id": 2, "name": "Bob", "salary": 4000}]',
            '102|WRITE|employees|[{"id": 2, "name": "Bob", "salary": 4000}]|None',
            "102|ABORT",
            '103|WRITE|employees|[{"id": 1, "name": "Alice", "salary": 5000}]|[{"id": 1, "name": "Alice", "salary": 6000}]',
        ]

        table = Table("employees")

        before_states = Row(table, PrimaryKey(None), None)
        after_states = Row(
            table, PrimaryKey(None), [{"id": 2, "name": "Bob", "salary": 4000}]
        )

        transaction_action = TransactionAction(
            102, "WRITE", "row", after_states, before_states
        )

        frm.write_log(transaction_action)
        self.assertEqual(int((frm._wa_logs[4]).split("|")[0]), 102)
        self.assertEqual(frm._wa_logs[4].split("|")[1], "WRITE")
        self.assertEqual(frm._wa_logs[4].split("|")[2], "employees")
        self.assertEqual(frm._wa_logs[4].split("|")[3], "None")
        self.assertEqual(
            frm._wa_logs[4].split("|")[4],
            json.dumps([{"id": 2, "name": "Bob", "salary": 4000}]),
        )

    @patch.object(FailureRecoveryManager, "_start_checkpoint_cron_job")
    @patch("StorageManager.classes.StorageManager")
    def test_save_checkpoint(
        self, mock_storage_manager, mock_start_checkpoint_cron_job
    ):
        # Setup
        mock_start_checkpoint_cron_job.return_value = None
        mock_storage = mock_storage_manager()
        mock_storage.write_block_to_disk = MagicMock()
        mock_storage.write_hash_block_to_disk = MagicMock()

        # Create test buffer data
        test_buffer = {
            ("table1", "1"): b"block1_data",
            ("table2", "2"): b"block2_data",
            ("hash", 1, "table3", 3, "column1"): b"hash_block_data",
        }

        # Create test write-ahead logs
        test_wa_logs = [
            "101|START",
            '101|WRITE|employees|None|[{"id": 1, "name": "Alice", "salary": 5000}]',
            "101|COMMIT",
        ]

        # Initialize FRM with mocked components
        with open("./FailureRecoveryManager/test/test_log.log", "w") as file:
            pass  # Create empty test log file

        buffer = Buffer(5)
        buffer.clear_buffer = MagicMock(return_value=test_buffer)

        frm = FailureRecoveryManager(
            buffer=buffer,
            log_file="./FailureRecoveryManager/test/test_log.log",
            storage_manager=mock_storage,
        )
        frm._wa_logs = test_wa_logs.copy()

        # Execute
        frm._save_checkpoint()

        # Assert buffer operations
        buffer.clear_buffer.assert_called_once()

        # Verify normal block writes
        mock_storage.write_block_to_disk.assert_any_call(
            table="table1", block_id=1, block_data=b"block1_data"
        )
        mock_storage.write_block_to_disk.assert_any_call(
            table="table2", block_id=2, block_data=b"block2_data"
        )

        # Verify hash block writes
        mock_storage.write_hash_block_to_disk.assert_called_once_with(
            table="table3",
            block_id=3,
            block_data=b"hash_block_data",
            column="column1",
            hash_value=1,
        )

        # Verify logs were written to file
        with open("./FailureRecoveryManager/test/test_log.log", "r") as f:
            log_content = f.read().strip().split("\n")

        # Check that all logs were written and a checkpoint was added
        self.assertEqual(len(log_content), 4)  # 3 logs + 1 checkpoint
        for i in range(3):
            self.assertEqual(log_content[i], test_wa_logs[i])

        # Check checkpoint format
        self.assertTrue(log_content[-1].startswith("CHECKPOINT|"))

        # Verify write-ahead logs were cleared
        self.assertEqual(len(frm._wa_logs), 0)

        # Cleanup
        with open("./FailureRecoveryManager/test/test_log.log", "w") as file:
            pass

    @patch.object(FailureRecoveryManager, "_start_checkpoint_cron_job")
    @patch("StorageManager.classes.StorageManager")
    def test_save_checkpoint_empty_logs(
        self, mock_storage_manager, mock_start_checkpoint_cron_job
    ):
        # Setup for empty logs case
        mock_start_checkpoint_cron_job.return_value = None
        mock_storage = mock_storage_manager()

        buffer = Buffer(5)
        buffer.clear_buffer = MagicMock(return_value=None)

        frm = FailureRecoveryManager(
            buffer=buffer,
            log_file="./FailureRecoveryManager/test/test_log.log",
            storage_manager=mock_storage,
        )
        frm._wa_logs = []

        # Execute
        frm._save_checkpoint()

        # Assert
        buffer.clear_buffer.assert_called_once()
        self.assertEqual(len(frm._wa_logs), 0)

        with open("./FailureRecoveryManager/test/test_log.log", "r") as f:
            self.assertEqual(f.read().strip(), "")

        # Cleanup
        with open("./FailureRecoveryManager/test/test_log.log", "w") as file:
            pass

    # test_recover
    @patch.object(FailureRecoveryManager, "_start_checkpoint_cron_job")
    @patch("StorageManager.classes.StorageManager")
    def test_recover(self, mock_storage_manager, mock_start_checkpoint_cron_job):
        with open("./FailureRecoveryManager/test/log_recover.log", "w") as file:
            pass

        log_data = [
            'CHECKPOINT|[]',
            '101|START',
            '101|WRITE|Student|None|[{"StudentID": 1, "FullName": "Alice", "GPA": 4}]',
            "101|COMMIT",
            "102|START",
            "103|START",
            '103|WRITE|Student|[{"StudentID": 2, "FullName": "John", "GPA": 3}]|[{"StudentID": 2, "FullName": "John B", "GPA": 4}]',
            'CHECKPOINT|[102,103]',
        ]

        with open("./FailureRecoveryManager/test/log_recover.log", "w") as log_file:
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
                    "\n====================================\n",
                ),
                1,
            )[1]
        )

        mock_storage.delete_block = MagicMock(
            side_effect=lambda data_delete: (
                print(
                    "\n=========DELETE_BLOCK================",
                    f"\ntable: {data_delete.table}",
                    f"\nCondition: {list(map(lambda v: v.__dict__, data_delete.conditions))}",
                    "\n====================================\n",
                ),
                1,
            )[1]
        )

        frm = FailureRecoveryManager(
            buffer=Buffer(5),
            log_file="./FailureRecoveryManager/test/log_recover.log",
            storage_manager=mock_storage,
        )
        frm._wa_logs = [
            '102|WRITE|Student|None|[{"StudentID": 2, "FullName": "Bob", "GPA": 2}]',
            '102|WRITE|Student|[{"StudentID": 2, "FullName": "Bob", "GPA": 2}]|None',
            "102|ABORT",
            '103|WRITE|Student|[{"StudentID": 1, "FullName": "Alice", "GPA": 1}]|[{"StudentID": 1, "FullName": "Alice", "GPA": 2}]',
        ]

        criteria = RecoverCriteria(transaction_id=103)

        frm.recover(criteria)

        print("wa_logs_last: ", frm._wa_logs)

        self.assertEqual(len(frm._wa_logs), 7)
        # mock_read_lines_from_end.assert_called_once()
        self.assertEqual(mock_storage.write_block.call_count, 2)

        expected_logs = [
            '102|WRITE|Student|None|[{"StudentID": 2, "FullName": "Bob", "GPA": 2}]',
            '102|WRITE|Student|[{"StudentID": 2, "FullName": "Bob", "GPA": 2}]|None',
            "102|ABORT",
            '103|WRITE|Student|[{"StudentID": 1, "FullName": "Alice", "GPA": 1}]|[{"StudentID": 1, "FullName": "Alice", "GPA": 2}]',
            '103|WRITE|Student|[{"StudentID": 1, "FullName": "Alice", "GPA": 2}]|[{"StudentID": 1, "FullName": "Alice", "GPA": 1}]',
            '103|WRITE|Student|[{"StudentID": 2, "FullName": "John B", "GPA": 4}]|[{"StudentID": 2, "FullName": "John", "GPA": 3}]',
            "103|ABORT",
        ]

        self.assertEqual(frm._wa_logs, expected_logs)

        with open("./FailureRecoveryManager/test/log_recover.log", "r") as f:
            actual_content = f.read()

        self.assertEqual(actual_content.strip(), "\n".join(log_data + expected_logs))
        with open("./FailureRecoveryManager/test/log_recover.log", "w") as file:
            pass

    # recover_system_crash
    @patch.object(FailureRecoveryManager, "_start_checkpoint_cron_job")
    @patch("StorageManager.classes.StorageManager")
    def test_recover_system_crash(
        self, mock_storage_manager, mock_start_checkpoint_cron_job
    ):
        log_data = [
            'CHECKPOINT|[]',
            '101|START',
            '101|WRITE|Student|None|[{"StudentID": 1, "FullName": "Alice", "GPA": 4}]',
            "101|COMMIT",
            "102|START",
            "103|START",
            '103|WRITE|Student|[{"StudentID": 2, "FullName": "John", "GPA": 3}]|[{"StudentID": 2, "FullName": "John B", "GPA": 4}]',
            'CHECKPOINT|[102,103]',
            '102|WRITE|Student|None|[{"StudentID": 2, "FullName": "Bob", "GPA": 2}]',
            '102|WRITE|Student|[{"StudentID": 2, "FullName": "Bob", "GPA": 2}]|None',
            "102|ABORT",
            '103|WRITE|Student|[{"StudentID": 1, "FullName": "Alice", "GPA": 1}]|[{"StudentID": 1, "FullName": "Alice", "GPA": 2}]',
        ]

        with open(
            "./FailureRecoveryManager/test/log_recover_crash.log", "w"
        ) as log_file:
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
                    "\n====================================\n",
                ),
                1,
            )[1]
        )

        mock_storage.delete_block = MagicMock(
            side_effect=lambda data_delete: (
                print(
                    "\n=========DELETE_BLOCK================",
                    f"\ntable: {data_delete.table}",
                    f"\nCondition: {list(map(lambda v: v.__dict__, data_delete.conditions))}",
                    "\n====================================\n",
                ),
                1,
            )[1]
        )

        frm = FailureRecoveryManager(
            buffer=Buffer(5),
            log_file="./FailureRecoveryManager/test/log_recover_crash.log",
            storage_manager=mock_storage,
        )

        frm.recover_system_crash()

        self.assertEqual(len(frm._wa_logs), 3)
        self.assertEqual(mock_storage.write_block.call_count, 4)
        self.assertEqual(mock_storage.delete_block.call_count, 1)

        expected_logs = [
            '103|WRITE|Student|[{"StudentID": 1, "FullName": "Alice", "GPA": 2}]|[{"StudentID": 1, "FullName": "Alice", "GPA": 1}]',
            '103|WRITE|Student|[{"StudentID": 2, "FullName": "John B", "GPA": 4}]|[{"StudentID": 2, "FullName": "John", "GPA": 3}]',
            "103|ABORT",
        ]

        self.assertEqual(frm._wa_logs, expected_logs)

        with open("./FailureRecoveryManager/test/log_recover_crash.log", "r") as f:
            actual_content = f.read()

        self.assertEqual(actual_content.strip(), "\n".join(log_data + expected_logs))
        with open("./FailureRecoveryManager/test/log_recover_crash.log", "w") as file:
            pass


if __name__ == "__main__":
    unittest.main()
