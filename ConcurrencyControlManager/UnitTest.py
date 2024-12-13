import unittest
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ConcurrencyControlManager.classes import TransactionAction, ConcurrencyControlManager, PrimaryKey, Row, Table, Cell, DataItem, Action, Response, WaitForGraph

class TestConcurrencyControlManager(unittest.TestCase):
    def setUp(self):
        self.ccm = ConcurrencyControlManager(algorithm="Tes")
    
    # Main Methods
    
    def test_begin_transaction(self):
        # [Tes-MainMethod] begin_transaction
        ccm = ConcurrencyControlManager(algorithm="Tes")
        transaction_id_1 = self.ccm.begin_transaction()
        transaction_id_2 = self.ccm.begin_transaction()
        self.assertNotEqual(transaction_id_1, transaction_id_2)
    
    def test_validate_object(self):
        # [Tes-MainMethod] validate_object
        row_tes = Row(Table('table'), PrimaryKey(1), {'col1': 1, 'col2': 'SBD'})
        row_tes2 = Row(Table('table'), PrimaryKey(1), {'col1': 1, 'col2': 'WBD'})
        tid1 = self.ccm.begin_transaction()
        tid2 = self.ccm.begin_transaction()
        
        # print("\n")
        
        # Tes: Wound
        # print("\nTest case 1:")
        ccm1 = ConcurrencyControlManager(algorithm="Tes")
        tid1 = ccm1.begin_transaction()
        tid2 = ccm1.begin_transaction()
        t_action1_read = TransactionAction(tid1,"read","row", row_tes, None)
        t_action2_read = TransactionAction(tid2,"read","row", row_tes, None)
        t_action1_write = TransactionAction(tid1,"write","row", row_tes, row_tes2)
        
        res1 = ccm1.validate_object(t_action1_read)  # T
        res2 = ccm1.validate_object(t_action2_read)  # T
        res3 = ccm1.validate_object(t_action1_write)  # T
        
        self.assertTrue(res1.allowed)
        self.assertEqual(res1.transaction_id, tid1)
        self.assertTrue(res2.allowed)
        self.assertEqual(res2.transaction_id, tid2)
        self.assertTrue(res3.allowed)
        self.assertEqual(res3.transaction_id, tid1)
        
        # Tes: Upgrade Lock
        # print("\nTest case 2:")
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
        # print("\nTest case 3:")
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
        # print("\nTest case 4:")
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
        # print("\nTest case 5:")
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
    
    def test_end_transaction(self):
        # [Tes-MainMethod] end_transaction
        row_tes = Row(Table('table'), PrimaryKey(1), {'col1': 1, 'col2': 'SBD'})
        row_tes2 = Row(Table('table'), PrimaryKey(1), {'col1': 1, 'col2': 'WBD'})
        ccm5 = ConcurrencyControlManager(algorithm="Tes")
        tid1 = ccm5.begin_transaction()
        tid2 = ccm5.begin_transaction()
        t_action1_read = TransactionAction(tid1,"read","row", row_tes, None)
        t_action2_read = TransactionAction(tid2,"read","row", row_tes, None)
        t_action1_write = TransactionAction(tid1,"write","row", row_tes2, row_tes)
        t_action2_write = TransactionAction(tid2,"write","row", row_tes2, row_tes)
        
        res0 = ccm5.validate_object(t_action1_read) # T
        res1 = ccm5.validate_object(t_action1_write) # T
        res2 = ccm5.validate_object(t_action2_write) # F
        
        self.assertTrue(res0.allowed)
        self.assertEqual(res0.transaction_id, tid1)
        self.assertEqual(res1.transaction_id, tid1)
        self.assertTrue(res1.allowed)
        self.assertFalse(res2.allowed)
        self.assertEqual(res2.transaction_id, tid2)
        
        ccm5.end_transaction(tid1, "success")
        res2 = ccm5.validate_object(t_action2_write) # T
        self.assertTrue(res2.allowed)
        
        tid3 = ccm5.begin_transaction()
        t_action3_read = TransactionAction(tid3,"read","row", row_tes, None)
        t_action3_write = TransactionAction(tid3,"write","row", row_tes, row_tes2)
        res3 = ccm5.validate_object(t_action3_write) # F
        res4 = ccm5.validate_object(t_action3_read) # F
        
        self.assertFalse(res3.allowed)
        self.assertFalse(res4.allowed)
        
        ccm5.end_transaction(tid2, "success")
        
        res3 = ccm5.validate_object(t_action3_write) # T
        res4 = ccm5.validate_object(t_action3_read) # T
        
        self.assertTrue(res3.allowed)
        self.assertTrue(res4.allowed)
        
    def test_susah(self):
        table1 = Table('table1')
        table2 = Table('table2')
        
        row1_table1 = Row(table1, PrimaryKey(1), {'col1': 1, 'col2': 'A'})
        row2_table1 = Row(table1, PrimaryKey(2), {'col1': 2, 'col2': 'A'})
        row3_table1 = Row(table1, PrimaryKey(3), {'col1': 1, 'col2': 'B'})
        
        row1_table2 = Row(table2, PrimaryKey(1), {'col1': 1, 'col2': 'A'})
        row2_table2 = Row(table2, PrimaryKey(2), {'col1': 1, 'col2': 'B'})
        row3_table2 = Row(table2, PrimaryKey(3), {'col1': 1, 'col2': 'A'})
        
        cell1_row1_table1 = Cell(table1, row1_table1, row1_table1.pkey, 'col1', 1)
        cell2_row1_table1 = Cell(table1, row1_table1, row1_table1.pkey, 'col2', 'A')
        cell1_row2_table1 = Cell(table1, row2_table1, row2_table1.pkey, 'col1', 2)
        cell2_row2_table1 = Cell(table1, row2_table1, row2_table1.pkey, 'col2', 'A')
        
        ccm = ConcurrencyControlManager(algorithm='tes')
        
        tid1 = ccm.begin_transaction()
        tid2 = ccm.begin_transaction()
        tid3 = ccm.begin_transaction()
        tid4 = ccm.begin_transaction()
        tid5 = ccm.begin_transaction()
        
        res1 = ccm.validate_object(TransactionAction(tid1, "read", "table", table1, None))
        res2 = ccm.validate_object(TransactionAction(tid2, "write", "row", row1_table1, None))
        res3 = ccm.validate_object(TransactionAction(tid3, "write", "row", row1_table1, None))
        res4 = ccm.validate_object(TransactionAction(tid4, "write", "row", row1_table1, None))
        res5 = ccm.validate_object(TransactionAction(tid5, "write", "row", row1_table1, None))
        
        res6 = ccm.validate_object(TransactionAction(tid1, "read", "row", row2_table2, None))
        res7 = ccm.validate_object(TransactionAction(tid2, "read", "row", row2_table2, None))
        res8 = ccm.validate_object(TransactionAction(tid3, "write", "row", row2_table2, None))
        res9 = ccm.validate_object(TransactionAction(tid4, "read", "row", row2_table2, None))
        res10 = ccm.validate_object(TransactionAction(tid5, "write", "row", row2_table2, None))

        self.assertTrue(res1.allowed)
        self.assertFalse(res2.allowed)
        self.assertFalse(res3.allowed)
        self.assertFalse(res4.allowed)
        self.assertFalse(res5.allowed)
        
        self.assertTrue(res6.allowed)
        self.assertFalse(res7.allowed)
        self.assertFalse(res8.allowed)
        self.assertFalse(res9.allowed)
        self.assertFalse(res10.allowed)
        
        self.assertEqual(res1.status, "success")
        self.assertEqual(res2.status, "wait")
        self.assertEqual(res3.status, "wait")
        self.assertEqual(res4.status, "wait")
        self.assertEqual(res5.status, "wait")
        self.assertEqual(res6.status, "success")
        self.assertEqual(res7.status, "wait")
        self.assertEqual(res8.status, "wait")
        self.assertEqual(res9.status, "wait")
        self.assertEqual(res9.status, "wait")
        self.assertEqual(len(ccm.waiting_list), 8)
        
        res10 = ccm.validate_object(TransactionAction(tid1, "commit", None, None, None))
        self.assertEqual(len(ccm.waiting_list), 6)
        
        res11 = ccm.validate_object(TransactionAction(tid4, "commit", None, None, None))
        self.assertEqual(len(ccm.waiting_list), 7)
        
        res11 = ccm.validate_object(TransactionAction(tid3, "commit", None, None, None))
        self.assertEqual(len(ccm.waiting_list), 8)
        
        res11 = ccm.validate_object(TransactionAction(tid2, "commit", None, None, None))
        self.assertEqual(len(ccm.waiting_list), 0)
        
        # for t_action in ccm.waiting_list:
        #     print(t_action.id, t_action.action)
        
    def test_deadlock(self):
        table1 = Table('table1')
        table2 = Table('table2')
        
        row1_table1 = Row(table1, PrimaryKey(1), {'col1': 1, 'col2': 'A'})
        row2_table1 = Row(table1, PrimaryKey(2), {'col1': 2, 'col2': 'A'})
        row3_table1 = Row(table1, PrimaryKey(3), {'col1': 1, 'col2': 'B'})
        
        row1_table2 = Row(table2, PrimaryKey(1), {'col1': 1, 'col2': 'A'})
        row2_table2 = Row(table2, PrimaryKey(2), {'col1': 1, 'col2': 'B'})
        row3_table2 = Row(table2, PrimaryKey(3), {'col1': 1, 'col2': 'A'})
        
        cell1_row1_table1 = Cell(table1, row1_table1, row1_table1.pkey, 'col1', 1)
        cell2_row1_table1 = Cell(table1, row1_table1, row1_table1.pkey, 'col2', 'A')
        cell1_row2_table1 = Cell(table1, row2_table1, row2_table1.pkey, 'col1', 2)
        cell2_row2_table1 = Cell(table1, row2_table1, row2_table1.pkey, 'col2', 'E')
        cell3_row2_table1 = Cell(table1, row2_table1, row2_table1.pkey, 'col3', 'B')
        cell4_row2_table1 = Cell(table1, row2_table1, row2_table1.pkey, 'col4', 'C')
        cell5_row2_table1 = Cell(table1, row2_table1, row2_table1.pkey, 'col5', 'D')
        
        ccm = ConcurrencyControlManager(algorithm='tes')
        
        tid1 = ccm.begin_transaction()
        tid2 = ccm.begin_transaction()
        tid3 = ccm.begin_transaction()
        tid4 = ccm.begin_transaction()
        tid5 = ccm.begin_transaction()
        
        res1 = ccm.validate_object(TransactionAction(tid1,"read", "cell", cell1_row1_table1, None))
        res2 = ccm.validate_object(TransactionAction(tid2,"read", "cell", cell2_row1_table1, None))
        res3 = ccm.validate_object(TransactionAction(tid3,"read", "cell", cell2_row1_table1, None))
        res4 = ccm.validate_object(TransactionAction(tid4,"read", "cell", cell2_row1_table1, None))
        res5 = ccm.validate_object(TransactionAction(tid5,"read", "cell", cell2_row1_table1, None))
        res6 = ccm.validate_object(TransactionAction(tid5,"read", "cell", cell1_row1_table1, None))
        res7 = ccm.validate_object(TransactionAction(tid4,"read", "cell", cell1_row1_table1, None))
        res8 = ccm.validate_object(TransactionAction(tid3,"read", "cell", cell1_row1_table1, None))
        res9 = ccm.validate_object(TransactionAction(tid2,"read", "cell", cell1_row1_table1, None))
        res10 = ccm.validate_object(TransactionAction(tid1,"read", "cell", cell2_row1_table1, None))
        res11 = ccm.validate_object(TransactionAction(tid2,"read", "cell", cell1_row2_table1, None))
        res12 = ccm.validate_object(TransactionAction(tid3,"read", "cell", cell1_row2_table1, None))
        res13 = ccm.validate_object(TransactionAction(tid4,"read", "cell", cell1_row2_table1, None))
        res14 = ccm.validate_object(TransactionAction(tid5,"read", "cell", cell1_row2_table1, None))
        res15 = ccm.validate_object(TransactionAction(tid2,"write", "cell", cell2_row2_table1, None))
        res16 = ccm.validate_object(TransactionAction(tid3,"write", "cell", cell3_row2_table1, None))
        res17 = ccm.validate_object(TransactionAction(tid4,"write", "cell", cell4_row2_table1, None))
        res18 = ccm.validate_object(TransactionAction(tid5,"write", "cell", cell5_row2_table1, None))
        res19 = ccm.validate_object(TransactionAction(tid5,"write", "cell", cell1_row1_table1, None))
        self.assertEqual(len(ccm.waiting_list),1)
        # print("LEN WAITING LIST1:", len(ccm.waiting_list))
        res20 = ccm.validate_object(TransactionAction(tid4,"write", "cell", cell2_row1_table1, None))
        self.assertEqual(len(ccm.waiting_list), 2)
        # print("LEN WAITING LIST2:", len(ccm.waiting_list))
        res21 = ccm.validate_object(TransactionAction(tid3,"write", "cell", cell1_row1_table1, None))
        self.assertEqual(len(ccm.waiting_list), 3)
        # print("LEN WAITING LIST3:", len(ccm.waiting_list))
        res22 = ccm.validate_object(TransactionAction(tid2,"write", "cell", cell2_row1_table1, None))
        self.assertEqual(len(ccm.waiting_list), 4)
        # print("LEN WAITING LIST4:", len(ccm.waiting_list))
        res23 = ccm.validate_object(TransactionAction(tid1,"write", "cell", cell1_row2_table1, None))
        self.assertEqual(len(ccm.waiting_list), 12) # ada 12 karena termasuk action "start"
        # print("LEN WAITING LIST5:", len(ccm.waiting_list))
        # for t_act in ccm.waiting_list:
            # print("TID:", t_act.id)
            # print("Action:", t_act.action)
        res24 = ccm.validate_object(TransactionAction(tid1,"commit", None, None, None))
        self.assertEqual(len(ccm.waiting_list), 6) # ada 12 karena termasuk action "start"
        # print("DATAITEM:", ccm.transaction_dataitem_map)
        # print("WAIT-FOR:", ccm.wait_for_graph.waitfor)
        # print("LOCK-X:", ccm.lock_X)
        # print("LEN WAITING LIST6:", len(ccm.waiting_list))
        # for t_act in ccm.waiting_list:
            # print("TID:", t_act.id)
            # print("Action:", t_act.action)
        
        self.assertTrue(res1.allowed)
        self.assertTrue(res2.allowed)
        self.assertTrue(res3.allowed)
        self.assertTrue(res4.allowed)
        self.assertTrue(res5.allowed)
        self.assertTrue(res6.allowed)
        self.assertTrue(res7.allowed)
        self.assertTrue(res8.allowed)
        self.assertTrue(res9.allowed)
        self.assertTrue(res10.allowed)
        self.assertTrue(res11.allowed)
        self.assertTrue(res12.allowed)
        self.assertTrue(res13.allowed)
        self.assertTrue(res14.allowed)
        self.assertTrue(res15.allowed)
        self.assertTrue(res16.allowed)
        self.assertTrue(res17.allowed)
        self.assertTrue(res18.allowed)
        self.assertFalse(res19.allowed)
        self.assertFalse(res20.allowed)
        self.assertFalse(res21.allowed)
        self.assertFalse(res22.allowed)
        self.assertTrue(res23.allowed)
        self.assertTrue(res24.allowed)
    
if __name__ == "__main__":
    unittest.main(verbosity=2)