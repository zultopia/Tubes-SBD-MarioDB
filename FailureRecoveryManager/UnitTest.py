import datetime
import unittest
from unittest.mock import MagicMock, patch

from FailureRecoveryManager.ExecutionResult import ExecutionResult
from FailureRecoveryManager.Rows import Rows

from .FailureRecoveryManager import FailureRecoveryManager, LRUCache


class TestFailureRecoveryManager(unittest.TestCase):
    # Go to root directory (\Tubes-SBD-MarioDB>)
    # Run  python -m "FailureRecoveryManager.UnitTest"
    def setUp(self):
        self.lru = LRUCache(5)
        self.frm = FailureRecoveryManager()

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

    @patch("builtins.open", new_callable=MagicMock)
    def test_save_checkpoint(self, mock_open):
        self.frm._wa_logs = [
            '102|WRITE|employees|None|[{"id": 2, "name": "Bob", "salary": 4000}]',
            '103|WRITE|employees|[{"id": 1, "name": "Alice", "salary": 5000}]|[{"id": 1, "name": "Alice", "salary": 6000}]',
        ]
        self.frm._save_checkpoint()
        mock_open.assert_called_once_with("./FailureRecoveryManager/log.log", "a")

    def test_write_log(self):
        execution_result = ExecutionResult(
            transaction_id=102,
            data_before=Rows(None),
            data_after=Rows([{"id": 2, "name": "Bob", "salary": 4000}]),
            status="WRITE",
            table="employees",
        )
        self.frm.write_log(execution_result)
        self.assertEqual(len(self.frm._wa_logs), 5)
        self.assertEqual(int((self.frm._wa_logs[4]).split("|")[0]), 102)
        self.assertEqual(self.frm._wa_logs[4].split("|")[1], "WRITE")
        self.assertEqual(self.frm._wa_logs[4].split("|")[2], "employees")
        self.assertEqual(self.frm._wa_logs[4].split("|")[3], "None")
        self.assertEqual(
            self.frm._wa_logs[4].split("|")[4],
            str({"id": 2, "name": "Bob", "salary": 4000}),
        )

    def test_is_wa_log_full_no_spare(self):
        self.frm._wa_logs = ["log"] * self.frm._max_size_log
        self.assertTrue(self.frm.is_wa_log_full())

    @patch("builtins.open", new_callable=MagicMock)
    @patch("datetime.datetime")
    def test_save_checkpoint(self, mock_datetime, mock_open):
        """Test to ensure checkpoint can be saved and contains correct log entries"""
        mock_datetime.now.return_value = datetime.datetime(2024, 12, 10, 10, 0, 0)
        mock_datetime.now.isoformat.return_value = "2024-12-10T10:00:00"

        self.frm._wa_logs.append("102|COMMIT")
        execution_result_1 = ExecutionResult(
            transaction_id=102,
            data_before=Rows(None),
            data_after=Rows([{"id": 2, "name": "Bob", "salary": 4000}]),
            status="WRITE",
            table="employees",
        )
        execution_result_2 = ExecutionResult(
            transaction_id=102,
            data_before=Rows(None),
            data_after=Rows(None),
            status="ABORT",
            table=None,
        )
        self.frm.write_log(execution_result_1)
        self.frm.write_log(execution_result_2)
        self.frm._save_checkpoint()
        self.assertEqual(self.frm._wa_logs, [])

    # test_recover
    # recover_system_crash


if __name__ == "__main__":
    unittest.main()
