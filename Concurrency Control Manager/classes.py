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
        self.lock_S = {} # Map Row -> List[transaction_id]
        self.lock_X = {} # Map Row -> transaction_id
        
    
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
        action = action.lower()
        if(action != "read" and action != "write"):
            raise ValueError(f"Invalid action type: {action}. Allowed actions are \"read\" or \"write\".")
        lockType = 'S' if action == "read" else 'X'
        
        # Suatu row didefinisikan oleh primary key dan tablenya
        primaryKey = object.pkey.keys
        table = object.table
        row = str(primaryKey) + table
        
        if lockType == 'S' and (row in self.lock_S and transaction_id in self.lock_S[row]):
            print(f"Transaction {transaction_id} already has lock-S")
            return Response(True, transaction_id)
        
        if lockType == 'X' and (row in self.lock_X and self.lock_X[row] == transaction_id):
            print(f"Transaction {transaction_id} already has lock-X")
            return Response(True, transaction_id)
        
        allowed = False
        if lockType == 'S':
            # Kalo minta lock-S, di-grant kalo gaada yg lagi megang lock-X (kecuali transaction itu sendiri)
            if row not in self.lock_X or self.lock_X[row] == transaction_id:
                allowed = True
        else:
            # Kalo minta lock-X, di-grant kalo gaada yg lagi megang lock-S maupun lock-X (kecuali transaction itu sendiri)
            if ((row not in self.lock_S or len(self.lock_S[row]) == 1 and self.lock_S[row] == [transaction_id])
                and row not in self.lock_X):
                allowed = True
                
        if not allowed:
            message = "Another transaction is holding lock-X"
            if lockType == 'X':
                message += " and/or lock-S"
            
            print(f"Lock-{lockType} is not granted to {transaction_id}: {message}")
            return Response(False, transaction_id)
        
        if lockType == 'S':
            if row in self.lock_X and self.lock_X[row] == transaction_id:
                print(f"{transaction_id} already has lock-X; all locks must be held till transaction commits")
            else:
                print(f"Lock-S is granted to {transaction_id}")
                if row not in self.lock_S:
                    self.lock_S[row] = [transaction_id]
                else:  
                    self.lock_S[row].append(transaction_id)
    
        else:
            if row in self.lock_S and transaction_id in self.lock_S[row]:
                print(f"{transaction_id} successfully upgrades from lock-S to lock-X")
                self.lock_S[row].remove(transaction_id)
            else:
                print(f"Lock-X is granted to {transaction_id}")
                
            if row not in self.lock_X:
                self.lock_X[row] = transaction_id
            else:  
                self.lock_X[row].append(transaction_id)
        
        return Response(True, transaction_id)        
    
    def end_transaction(self, transaction_id: int):
        # Flush objects of a particular transaction after it has successfully committed/aborted
        # Terminates the transaction
        pass

class WaitForGraph:
    def __init__(self):
        self.waitfor = {}   # tid_waiting: tid_waited; tid_waiting -> tid_waited; 1-to-1 or many-to-1
    
    def addEdge(self, tid_waiting: int, tid_waited: int):
        self.waitfor[tid_waiting] = tid_waited
    
    def deleteEdge(self, tid_waiting: int, tid_waited: int):
        del self.waitfor[tid_waiting]
        
    def deleteNode(self, tid): 
        deleted_node = []

        for tid_waiting, tid_waited in self.waitfor.items():
            if tid_waited == tid:
                deleted_node.append(tid_waiting)

        for key in deleted_node:
            del self.waitfor[key]
                
    def isCyclic(self):
        for tid_waiting, tid_waited in self.waitfor.items():
            visited_node = []
            current_node = tid_waiting
            while(current_node in self.waitfor):
                visited_node.append(current_node)
                current_node = self.waitfor[current_node]
                if(current_node == tid_waiting):
                    return True
        return False
    
    def __str__(self):
        return f"{self.waitfor}"
            
        
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

# wfg = WaitForGraph()
# wfg.addEdge(1, 2)
# wfg.addEdge(4, 1)
# wfg.addEdge(2, 3)
# wfg.addEdge(3, 5)

# print(wfg.isCyclic())

# wfg.addEdge(5, 1)
# print(wfg.isCyclic())

# wfg.deleteEdge(5, 1)
# print(wfg)

# wfg.deleteEdge(4, 1)
# wfg.addEdge(4, 5)

# print(wfg)

# wfg.deleteNode(5)
# print(wfg)