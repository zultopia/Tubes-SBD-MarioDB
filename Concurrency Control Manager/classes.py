# import class Row
from datetime import datetime
import time

class PrimaryKey:
    def __init__(self, *keys):
        # keys nya langsung value si primary key nya aja
        self.isComposite = len(keys) > 1
        self.keys = keys

    def __eq__(self, other):
        if not isinstance(other, PrimaryKey):
            return NotImplemented
        if(self.isComposite != other.isComposite):
            return False
        return self.keys == other.keys

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return (
            f"===== PrimaryKey =====\nisComposite: {self.isComposite}\nkeys: {self.keys}\n======================\n"
        )

class Row:
    def __init__(self, table: str, pkey: PrimaryKey, map: dict):
        self.table = table
        self.pkey = pkey
        self.map = map
        
    def __getitem__(self, key):
        # use case: Row['key1'], then it will return the value of that 'key1' key in the map
        if key in self.map:
            return self.map[key]
        raise KeyError(f"Key '{key}' not found")
    
    def __eq__(self, other):
        return (self.pkey == other.pkey) and (self.table == other.table)
    
    def __ne__(self, other):
        return not self.__eq__(other)
    
    def __str__(self):
        return f"=============== Row ===============\ntable: {self.table}\nmap:\n{self.map}\npkey:\n{self.pkey}===================================\n"

class Action:
    def __init__(self, action):
        self.action = action
        
    def __str__(self):
        return f"=== Action ===\naction: {self.action}\n==============\n"
        
class Response:
    def __init__(self, allowed: bool, transaction_id: int):
        self.allowed = allowed
        self.transaction_id = transaction_id
        
    def __str__(self):
        return f"==== Response ====\nallowed: {self.allowed}\ntransaction_id: {self.transaction_id}\n==================\n"

class Lock:
    def __init__(self, type: str, transaction_id: int, row: Row):
        # type is either 'S' or 'X'
        self.type = type.upper()
        if(self.type != 'S' and self.type != 'X'):
            raise ValueError(f"Invalid lock type: {type}. Allowed values are 'S' or 'X'.")
        self.transaction_id = transaction_id
        self.row = row
        
    def __str__(self):
        return f"=============== Lock ===============\ntype: {self.type}\ntransaction_id: {self.transaction_id}\nrow:\n{self.row}====================================\n"

class ConcurrencyControlManager:
    def __init__(self, algorithm: str):
        self.algorithm = algorithm
        self.activeLocks = {} # transaction_id: Lock[]
        
    
    def __str__(self):
        return f"===== ConcurrencyControlManager =====\nalgorithm: {self.algorithm}\n=====================================\n"
    
    def __generate_id(self) -> int:
        # return int(f"{datetime.now().strftime("%Y%m%d%H%M%S%f")}{random.randint(10000, 99999)}")
        return int((str(time.perf_counter()*1000000000)).replace('.', ''))
    
    def begin_transaction(self) -> int:
        # will return transaction_id: int
        return self.__generate_id()
    
    def log_object(self, object: Row, transaction_id: int):
        # implement lock on an object
        # assign timestamp on the object
        pass
    
    def validate_object(self, object: Row, transaction_id: int, action: Action) -> Response:
        # decide wether the object is allowed to do a particular action or not
        action = action.lower()
        if(action != "read" and action != "write"):
            raise ValueError(f"Invalid action type: {action}. Allowed actions are \"read\" or \"write\".")
        lockType = 'S' if action == "read" else 'X'
        
        for trans_id, lock_list in self.activeLocks.items():
            for idx,activeLock in enumerate(lock_list):
                # Row nya udah ada yg nge-lock?
                if(object == activeLock.row):
                    # Row di lock di transaksi yang berbeda?
                    if(transaction_id != trans_id):
                        if(lockType != activeLock.type):
                            print("Lock not granted S != X")
                            return Response(False, transaction_id)
                        else:
                            if(lockType == 'X'):
                                print("Lock not granted X == X")
                                return Response(False, transaction_id)
                            else:
                                break
                else:
                    continue
                              
        # Row tidak di-lock transaksi lain
        
        if(transaction_id in self.activeLocks):
            isAlrExist = False
            # Transaksi saat ini udah pernah ambil lock?
            for idx, lock in enumerate(self.activeLocks[transaction_id]):
                if(object == lock.row):
                    isAlrExist = True
                    
                    # Upgrade lock?
                    if(lockType == 'X' and lock.type == 'S'):
                        self.activeLocks[transaction_id][idx].type = 'X'
                        print("Lock Upgraded")
                    else:
                        print(f"Lock {lock.type} already granted")
                            
            if not isAlrExist:
                newLock = Lock(lockType, transaction_id, object)
                self.activeLocks[transaction_id].append(newLock)
                print(f"Lock {lockType} Acquired")
        else:
            newLock = Lock(lockType, transaction_id, object)
            self.activeLocks[transaction_id] = [newLock]
            print(f"Lock {lockType} Acquired")
        return Response(True, transaction_id)
    
    def end_transaction(self, transaction_id: int):
        # Flush objects of a particular transaction after it has successfully committed/aborted
        # Terminates the transaction
        pass
    
# ccm = ConcurrencyControlManager(algorithm="Test")
# print(ccm.begin_transaction())
# print(ccm.begin_transaction())
# print(ccm.begin_transaction())

# pkey = PrimaryKey("lala", 1)
# pkey1 = PrimaryKey("lala", 1)
# print(pkey1)
# pkey2 = PrimaryKey("lala")
# print(pkey != pkey1)
# print(pkey == pkey2)

# row = Row(table="table", pkey=pkey, map={'att1': 15})
# print(row)
# row1 = Row(table="table", pkey=pkey, map={'att1': 15})
# row2 = Row(table="table", pkey=pkey2, map={'att1': 15})
# row3 = Row(table="table1", pkey=pkey, map={'att1': 15, 'att2': "16"})
# print("=== tes Row comparison ===")
# print(row == row1)
# print(row == row2)
# print(row == row3)
# print(row != row1)
# print(row != row2)
# print(row != row3)

# print(row3['att1'])
# print(row3['att2'])

# lock = Lock('S',2,row)
# print(lock)

# row_tes_1 = Row('table', 1, {'col1': 1, 'col2': 'SBD'})
# tid1 = ccm.begin_transaction()
# tid2 = ccm.begin_transaction()

# print(ccm.validate_object(row_tes_1, tid1,'ReAd'))  # T
# print(ccm.validate_object(row_tes_1, tid2,'reAd'))  # T
# print(ccm.validate_object(row_tes_1, tid1,'wriTe')) # F

# print(ccm.validate_object(row_tes_1, tid1,'ReAd'))  # T
# print(ccm.validate_object(row_tes_1, tid1,'wriTe')) # T
# print(ccm.validate_object(row_tes_1, tid2,'reAd'))  # F

# print(ccm.validate_object(row_tes_1, tid1,'wriTe')) # T
# print(ccm.validate_object(row_tes_1, tid1,'ReAd'))  # T
# print(ccm.validate_object(row_tes_1, tid2,'reAd'))  # F

# print(ccm.validate_object(row_tes_1, tid1,'rEAd'))  # T
# print(ccm.validate_object(row_tes_1, tid2,'ReAd'))  # T
# print(ccm.validate_object(row_tes_1, tid1,'reAd'))  # T

# print(ccm.validate_object(row_tes_1, tid1,'wriTe')) # T
# print(ccm.validate_object(row_tes_1, tid2,'wriTe')) # F