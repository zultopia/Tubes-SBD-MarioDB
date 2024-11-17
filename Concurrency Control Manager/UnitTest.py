import unittest
from classes import ConcurrencyControlManager, PrimaryKey, Row, Action, Response

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
        row_tes = Row('table', 1, {'col1': 1, 'col2': 'SBD'})
        tid1 = self.ccm.begin_transaction()
        tid2 = self.ccm.begin_transaction()
        
        # Tes: Lock S == S (T); Lock S != X (F)
        ccm1 = ConcurrencyControlManager(algorithm="Tes")
        
        res1 = ccm1.validate_object(row_tes, tid1,'ReAd')  # T
        res2 = ccm1.validate_object(row_tes, tid2,'reAd')  # T
        res3 = ccm1.validate_object(row_tes, tid1,'wriTe')  # F
        
        self.assertTrue(res1.allowed)
        self.assertEqual(res1.transaction_id, tid1)
        self.assertTrue(res2.allowed)
        self.assertEqual(res2.transaction_id, tid2)
        self.assertFalse(res3.allowed)
        self.assertEqual(res3.transaction_id, tid1)
        
        # Tes: Upgrade Lock
        ccm2 = ConcurrencyControlManager(algorithm="Tes")
        
        res1 = ccm2.validate_object(row_tes, tid1,'ReAd')  # T
        res2 = ccm2.validate_object(row_tes, tid1,'wriTe') # T
        res3 = ccm2.validate_object(row_tes, tid2,'reAd')  # F
        
        self.assertTrue(res1.allowed)
        self.assertEqual(res1.transaction_id, tid1)
        self.assertTrue(res2.allowed)
        self.assertEqual(res2.transaction_id, tid1)
        self.assertFalse(res3.allowed)
        self.assertEqual(res3.transaction_id, tid2)
        
        # Tes: Lock X Already Granted
        ccm3 = ConcurrencyControlManager(algorithm="Tes")
        
        res1 = ccm3.validate_object(row_tes, tid1,'wriTe') # T
        res2 = ccm3.validate_object(row_tes, tid1,'ReAd')  # T
        res3 = ccm3.validate_object(row_tes, tid2,'reAd')  # F
        
        self.assertTrue(res1.allowed)
        self.assertEqual(res1.transaction_id, tid1)
        self.assertTrue(res2.allowed)
        self.assertEqual(res2.transaction_id, tid1)
        self.assertFalse(res3.allowed)
        self.assertEqual(res3.transaction_id, tid2)
        
        # Tes: Lock S Already Granted; Lock S == S (T)
        ccm4 = ConcurrencyControlManager(algorithm="Tes")
        
        res1 = ccm4.validate_object(row_tes, tid1,'rEAd')  # T
        res2 = ccm4.validate_object(row_tes, tid2,'ReAd')  # T
        res3 = ccm4.validate_object(row_tes, tid1,'reAd')  # T
        
        self.assertTrue(res1.allowed)
        self.assertEqual(res1.transaction_id, tid1)
        self.assertTrue(res2.allowed)
        self.assertEqual(res2.transaction_id, tid2)
        self.assertTrue(res3.allowed)
        self.assertEqual(res3.transaction_id, tid1)
        
        # Tes: Lock X == X (F)
        ccm5 = ConcurrencyControlManager(algorithm="Tes")
    
        res1 = ccm5.validate_object(row_tes, tid1,'wriTe') # T
        res2 = ccm5.validate_object(row_tes, tid2,'wriTe') # F
    
        self.assertTrue(res1.allowed)
        self.assertEqual(res1.transaction_id, tid1)
        self.assertFalse(res2.allowed)
        self.assertEqual(res2.transaction_id, tid2)
    
    def test_log_object(self):
        # [Tes-MainMethod] log_object
        pass
    
    def test_end_transaction(self):
        # [Tes-MainMethod] end_transaction
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

if __name__ == "__main__":
    unittest.main(verbosity=2)
