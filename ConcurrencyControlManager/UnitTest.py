import unittest
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ConcurrencyControlManager.classes import TransactionAction, ConcurrencyControlManager, PrimaryKey, Row, Table, Cell, DataItem, Action, Response, WaitForGraph

class TestConcurrencyControlManager(unittest.TestCase):
    def setUp(self):
        self.ccm = ConcurrencyControlManager(algorithm="Tes")
        self.table = Table("TestTable")
        self.row = Row(self.table, PrimaryKey("id"), {"column1": "value1"})
        self.cell = Cell(self.row, PrimaryKey("id"), "column1", "value1")
    
    # Main Methods
    
    def test_begin_transaction(self):
        # [Tes-MainMethod] begin_transaction
        ccm = ConcurrencyControlManager(algorithm="Tes")
        transaction_id_1 = self.ccm.begin_transaction()
        transaction_id_2 = self.ccm.begin_transaction()
        self.assertNotEqual(transaction_id_1, transaction_id_2)
    
    def test_validate_object(self):
        # [Tes-MainMethod] validate_object
        row_tes = Row('table', PrimaryKey(1), {'col1': 1, 'col2': 'SBD'})
        row_tes2 = Row('table', PrimaryKey(1), {'col1': 1, 'col2': 'WBD'})
        tid1 = self.ccm.begin_transaction()
        tid2 = self.ccm.begin_transaction()
        
        print("\n")
        
        # Tes: Lock S == S (T); Lock S != X (F)
        print("\nTest case 1:")
        ccm1 = ConcurrencyControlManager(algorithm="Tes")
        tid1 = ccm1.begin_transaction()
        tid2 = ccm1.begin_transaction()
        t_action1_read = TransactionAction(tid1,"read","row", row_tes, None)
        t_action2_read = TransactionAction(tid2,"read","row", row_tes, None)
        t_action1_write = TransactionAction(tid1,"write","row", row_tes, row_tes2)
        
        res1 = ccm1.validate_object(t_action1_read)  # T
        res2 = ccm1.validate_object(t_action2_read)  # T
        res3 = ccm1.validate_object(t_action1_write)  # F
        
        self.assertTrue(res1.allowed)
        self.assertEqual(res1.transaction_id, tid1)
        self.assertTrue(res2.allowed)
        self.assertEqual(res2.transaction_id, tid2)
        self.assertFalse(res3.allowed)
        self.assertEqual(res3.transaction_id, tid1)
        
        # Tes: Upgrade Lock
        print("\nTest case 2:")
        ccm2 = ConcurrencyControlManager(algorithm="Tes")
        tid1 = ccm2.begin_transaction()
        tid2 = ccm2.begin_transaction()
        t_action1_read = TransactionAction(tid1,"read","row", row_tes, None)
        t_action2_read = TransactionAction(tid2,"read","row", row_tes, None)
        t_action1_write = TransactionAction(tid1,"write","row", row_tes, row_tes2)
        
        res1 = ccm2.validate_object(t_action1_read)  # T
        res2 = ccm2.validate_object(t_action1_write) # T
        res3 = ccm2.validate_object(t_action2_read)  # F
        
        self.assertTrue(res1.allowed)
        self.assertEqual(res1.transaction_id, tid1)
        self.assertTrue(res2.allowed)
        self.assertEqual(res2.transaction_id, tid1)
        self.assertFalse(res3.allowed)
        self.assertEqual(res3.transaction_id, tid2)
        
        # Tes: Lock X Already Granted
        print("\nTest case 3:")
        ccm3 = ConcurrencyControlManager(algorithm="Tes")
        tid1 = ccm3.begin_transaction()
        tid2 = ccm3.begin_transaction()
        t_action1_read = TransactionAction(tid1,"read","row", row_tes, None)
        t_action2_read = TransactionAction(tid2,"read","row", row_tes, None)
        t_action1_write = TransactionAction(tid1,"write","row", row_tes, row_tes2)
        
        res1 = ccm3.validate_object(t_action1_write) # T
        res2 = ccm3.validate_object(t_action1_read)  # T
        res3 = ccm3.validate_object(t_action2_read)  # F
        
        self.assertTrue(res1.allowed)
        self.assertEqual(res1.transaction_id, tid1)
        self.assertTrue(res2.allowed)
        self.assertEqual(res2.transaction_id, tid1)
        self.assertFalse(res3.allowed)
        self.assertEqual(res3.transaction_id, tid2)
        
        # Tes: Lock S Already Granted; Lock S == S (T)
        print("\nTest case 4:")
        ccm4 = ConcurrencyControlManager(algorithm="Tes")
        tid1 = ccm4.begin_transaction()
        tid2 = ccm4.begin_transaction()
        t_action1_read = TransactionAction(tid1,"read","row", row_tes, None)
        t_action2_read = TransactionAction(tid2,"read","row", row_tes, None)
        t_action1_write = TransactionAction(tid1,"write","row", row_tes, row_tes2)
        
        res1 = ccm4.validate_object(t_action1_read)  # T
        res2 = ccm4.validate_object(t_action2_read)  # T
        res3 = ccm4.validate_object(t_action1_read)  # T
        
        self.assertTrue(res1.allowed)
        self.assertEqual(res1.transaction_id, tid1)
        self.assertTrue(res2.allowed)
        self.assertEqual(res2.transaction_id, tid2)
        self.assertTrue(res3.allowed)
        self.assertEqual(res3.transaction_id, tid1)
        
        # Tes: Lock X == X (F)
        print("\nTest case 5:")
        ccm5 = ConcurrencyControlManager(algorithm="Tes")
        tid1 = ccm5.begin_transaction()
        tid2 = ccm5.begin_transaction()
        t_action1_write = TransactionAction(tid1,"write","row", row_tes, row_tes2)
        t_action2_write = TransactionAction(tid2,"write","row", row_tes, row_tes2)
    
        res1 = ccm5.validate_object(t_action1_write) # T
        res2 = ccm5.validate_object(t_action2_write) # F
    
        self.assertTrue(res1.allowed)
        self.assertEqual(res1.transaction_id, tid1)
        self.assertFalse(res2.allowed)
        self.assertEqual(res2.transaction_id, tid2)
    
    def test_log_object(self):
        # [Tes-MainMethod] log_object
        pass
    
    def test_end_transaction(self):
        # [Tes-MainMethod] end_transaction
        row_tes = Row('table', PrimaryKey(1), {'col1': 1, 'col2': 'SBD'})
        row_tes2 = Row('table', PrimaryKey(1), {'col1': 1, 'col2': 'WBD'})
        ccm5 = ConcurrencyControlManager(algorithm="Tes")
        tid1 = ccm5.begin_transaction()
        tid2 = ccm5.begin_transaction()
        t_action1_read = TransactionAction(tid1,"read","row", row_tes, None)
        t_action2_read = TransactionAction(tid2,"read","row", row_tes, None)
        t_action1_write = TransactionAction(tid1,"write","row", row_tes, row_tes2)
        t_action2_write = TransactionAction(tid2,"write","row", row_tes, row_tes2)
        
        res0 = ccm5.validate_object(t_action1_read) # T
        res1 = ccm5.validate_object(t_action1_write) # T
        res2 = ccm5.validate_object(t_action2_write) # F
        
        self.assertTrue(res0.allowed)
        self.assertEqual(res0.transaction_id, tid1)
        self.assertTrue(res1.allowed)
        self.assertEqual(res1.transaction_id, tid1)
        self.assertFalse(res2.allowed)
        self.assertEqual(res2.transaction_id, tid2)
        
        ccm5.end_transaction(tid1)
        res2 = ccm5.validate_object(t_action2_write) # T
        self.assertTrue(res2.allowed)
        
        tid3 = ccm5.begin_transaction()
        t_action3_read = TransactionAction(tid3,"read","row", row_tes, None)
        t_action3_write = TransactionAction(tid3,"write","row", row_tes, row_tes2)
        res3 = ccm5.validate_object(t_action3_write) # F
        res4 = ccm5.validate_object(t_action3_read) # F
        
        self.assertFalse(res3.allowed)
        self.assertFalse(res4.allowed)
        
        ccm5.end_transaction(tid2)
        
        res3 = ccm5.validate_object(t_action3_write) # T
        res4 = ccm5.validate_object(t_action3_read) # T
        
        self.assertTrue(res3.allowed)
        self.assertTrue(res4.allowed)
        
        pass
    
    # Helper Methods
    
    # Helper Functionality
    
    def test_eq_PrimaryKey(self):
        # [Tes-Helper] compare equal Primary Key
        pass
    
    def test_ne_PrimaryKey(self):
        # [Tes-Helper] compare not equal Primary Key
        pass
    
    def test_eq_Row(self):
        # [Tes-Helper] compare equal Row
        pass
    
    def test_ne_Row(self):
        # [Tes-Helper] compare not equal Row
        pass
    
    # def test_deadlock_detection(self):
    #     wfg = WaitForGraph()
    #     wfg.addEdge(1, 2)
    #     wfg.addEdge(4, 1)
    #     wfg.addEdge(2, 3)
    #     wfg.addEdge(3, 5)
        
    #     # Tes: No Deadlock
    #     isDeadlock = wfg.isCyclic()
    #     self.assertFalse(isDeadlock)
        
    #     wfg.addEdge(5, 1)
        
    #     # Tes: Deadlock
    #     isDeadlock = wfg.isCyclic()
    #     self.assertTrue(isDeadlock)
        
    #     # Tes: delete node 5, transaction id 5 sudah commit
    #     wfg.deleteEdge(5, 1)
    #     wfg.deleteEdge(4, 1)
    #     wfg.addEdge(4, 5)
    #     wfg.deleteNode(5)
    #     lenWaitFor = len(wfg.waitfor)
    #     self.assertEqual(lenWaitFor, 2)

    def test_validate_object_read(self):
        tid1 = self.ccm.begin_transaction()
        row_tes = Row('table', PrimaryKey(1), {'col1': 1, 'col2': 'SBD'})
        t_action1_read = TransactionAction(tid1,"read","row", row_tes, None)
        response = self.ccm.validate_object(t_action1_read)
        self.assertTrue(response.allowed)
        self.assertEqual(response.transaction_id, tid1)
        self.assertIn(self.row, self.ccm.lock_S)
        self.assertIn(tid1, self.ccm.lock_S[self.row])

    def test_validate_object_write(self):
        tid1 = self.ccm.begin_transaction()
        row_tes = Row('table', PrimaryKey(1), {'col1': 1, 'col2': 'SBD'})
        row_tes2 = Row('table', PrimaryKey(1), {'col1': 1, 'col2': 'WBD'})
        t_action1_write = TransactionAction(tid1,"write","row", row_tes, row_tes2)
        response = self.ccm.validate_object(t_action1_write)
        self.assertTrue(response.allowed)
        self.assertEqual(response.transaction_id, tid1)
        self.assertIn(self.row, self.ccm.lock_X)
        self.assertEqual(self.ccm.lock_X[self.row], tid1)

    def test_validate_object_conflict(self):
        tid1 = self.ccm.begin_transaction()
        row_tes = Row('table', PrimaryKey(1), {'col1': 1, 'col2': 'SBD'})
        row_tes2 = Row('table', PrimaryKey(1), {'col1': 1, 'col2': 'WBD'})
        table_tes = Table('table')
        t_action1_read = TransactionAction(tid1,"read","table", table_tes, None)
        t_action1_write = TransactionAction(tid1,"write","row", row_tes, row_tes2)
        # Lock row with write by tid1
        self.ccm.validate_object(t_action1_write)
        # Try to read with tid2, should fail
        response = self.ccm.validate_object(t_action1_read)
        self.assertFalse(response.allowed)
        self.assertEqual(response.transaction_id, tid1)

    # def test_apply_lock_with_hierarchy_read(self):
    #     tid = self.ccm.begin_transaction()
    #     row_tes = Row('table', PrimaryKey(1), {'col1': 1, 'col2': 'SBD'})
    #     response = self.ccm.apply_lock_with_hierarchy(DataItem("Row", self.row), tid, "S")
    #     self.assertTrue(response.allowed)
    #     self.assertIn(self.row, self.ccm.lock_S)
    #     self.assertIn(tid, self.ccm.lock_S[self.row])

    # def test_apply_lock_with_hierarchy_write(self):
    #     tid = self.ccm.begin_transaction()
    #     response = self.ccm.apply_lock_with_hierarchy(DataItem("Row", self.row), tid, "X")
    #     self.assertTrue(response.allowed)
    #     self.assertIn(self.row, self.ccm.lock_X)
    #     self.assertEqual(self.ccm.lock_X[self.row], tid)

    # def test_apply_lock_with_hierarchy_conflict(self):
    #     tid1 = self.ccm.begin_transaction()
    #     tid2 = self.ccm.begin_transaction()
    #     self.ccm.apply_lock_with_hierarchy(DataItem("Row", self.row), tid1, "X")
    #     response = self.ccm.apply_lock_with_hierarchy(DataItem("Row", self.row), tid2, "S")
    #     self.assertFalse(response.allowed)

    # def test_check_lock_conflict_no_conflict(self):
    #     tid = self.ccm.begin_transaction()
    #     self.ccm.apply_lock_with_hierarchy(DataItem("Row", self.row), tid, "S")
    #     allowed, message = self.ccm.check_lock_conflict(self.row, "S")
    #     self.assertTrue(allowed)
    #     self.assertIsNone(message)

    # def test_check_lock_conflict_with_conflict(self):
    #     tid = self.ccm.begin_transaction()
    #     self.ccm.apply_lock_with_hierarchy(DataItem("Row", self.row), tid, "X")
    #     allowed, message = self.ccm.check_lock_conflict(self.row, "S")
    #     self.assertFalse(allowed)
    #     self.assertIsNotNone(message)
    
if __name__ == "__main__":
    unittest.main(verbosity=2)