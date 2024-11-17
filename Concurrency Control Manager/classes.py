# import class Row
from datetime import datetime
import random

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
        keys_list = ", ".join(map(str, self.keys))
        return (
            # f"PrimaryKey(keys=[{keys_list}], isComposite={self.isComposite})"
            f"{self.keys}"
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

class Action:
    def __init__(self, action):
        self.action = action
        
    def __str__(self):
        return f"=== Action ===\nAction: {self.action}\n==============\n"
        
class Response:
    def __init__(self, allowed: bool, transaction_id: int):
        self.allowed = allowed
        self.transaction_id = transaction_id
        
    def __str__(self):
        return f"==== Response ====\nallowed: {self.allowed}\ntransaction_id: {self.transaction_id}\n==================\n"

class ConcurrencyControlManager:
    def __init__(self, algorithm: str):
        self.algorithm = algorithm
    
    def __str__(self):
        return f"===== ConcurrencyControlManager =====\nalgorithm: {self.algorithm}\n=====================================\n"
    
    def __generate_id(self) -> int:
        return int(f"{datetime.now().strftime("%Y%m%d%H%M%S%f")}{random.randint(10000, 99999)}")
    
    def begin_transaction(self) -> int:
        # will return transaction_id: int
        
        return self.__generate_id()
    
    def log_object(self, object: Row, transaction_id: int):
        # implement lock on an object
        # assign timestamp on the object
        pass
    
    def validate_object(self, object: Row, transaction_id: int, action: Action) -> Response:
        # decide wether the object is allowed to do a particular action or not
        pass
    
    def end_transaction(self, transaction_id: int):
        # Flush objects of a particular transaction after it has successfully committed/aborted
        # Terminates the transaction
        pass
    
# ccm = ConcurrencyControlManager(algorithm="Test")
# print(ccm.begin_transaction())

# pkey = PrimaryKey("lala", 1)
# pkey1 = PrimaryKey("lala", 1)
# pkey2 = PrimaryKey("lala")
# print(pkey != pkey1)
# print(pkey == pkey2)

# row = Row(table="table", pkey=pkey, map={'att1': 15})
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
    