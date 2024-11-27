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

class Table:
    def __init__(self, table: str):
        self.table = table
    
    def __eq__(self, other):
        return self.table == other.table

    def __ne__(self, other):
        return not self.__eq__(other)

class Row:
    def __init__(self, table: Table, pkey: PrimaryKey, map: dict):
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

    def get_table(self): 
        return self.table

class Action:
    def __init__(self, action : str):
        self.action = action 
        
    def __str__(self):
        return f"=== Action ===\naction: {self.action}\n==============\n"
        
class Response:
    def __init__(self, allowed: bool, transaction_id: int, message: str = ""):
        self.allowed = allowed
        self.transaction_id = transaction_id
        self.message = message
        
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

class Cell:
    def __init__(self, row: Row, pkey: PrimaryKey, attribute: str, value: any):
        self.row = row
        self.pkey = pkey
        self.attribute = attribute
        self.value = value
    
    def __eq__(self, other):
        return (self.pkey == other.pkey) and (self.attribute == other.attribute) and (self.table == other.table)

    def get_row(self):
        return self.row
    
    def get_table(self):
        return self.row.get_table()
    
class DataItem:
    def __init__(self, level: str, data_item: Table | Row | Cell):
        self.level = level
        self.data_item = data_item
        
    def __eq__(self, other):
        return (self.level == other.level) and (self.data_item == other.data_item)
    
    def __ne__(self, other):
        return not self.__eq__(other)

class Transaction: 
    def __init__(self, tid: int, action: Action, level: int, data_item: str): 
        self.id = tid 
        self.action = action
        self.level  = level
        # TODO: update data_item
        self.data_item = data_item

class WaitForGraph:
    def __init__(self):
        self.waitfor = {}   # tid_waiting: set[tid_waited]
    
    def addEdge(self, tid_waiting: int, tid_waited: int):
        if tid_waiting not in self.waitfor: 
            self.waitfor[tid_waiting] = set()
        self.waitfor[tid_waiting].add(tid_waited)
    
    def deleteEdge(self, tid_waiting: int, tid_waited: int):
        if tid_waiting in self.waitfor and tid_waited in self.waitfor[tid_waiting]:
            self.waitfor[tid_waiting].remove(tid_waited)
            if not self.waitfor[tid_waiting]: 
                del self.waitfor[tid_waiting]
        
    def deleteNode(self, tid): 
        # remove edges that pointing to tid node 
        for tid_waiting in list(self.waitfor):
            if tid in self.waitfor[tid_waiting]: 
                self.waitfor[tid_waiting].remove(tid)

                if not self.waitfor[tid_waiting]: 
                    del self.waitfor[tid_waiting]

        # remove the tid node
        if tid in self.waitfor: 
            del self.waitfor[tid]
                
    def isCyclic(self):
        def dfs(node, visited, rec_stack): 
            visited.add(node)
            rec_stack.add(node)

            for neighbor in self.waitfor.get(node, []): 
                if neighbor not in visited: 
                    if dfs(neighbor, visited, rec_stack): 
                        return True
                elif neighbor in rec_stack: 
                    return True
                
            rec_stack.remove(node)
            return False

        visited = set()
        rec_stack = set()

        for node in self.waitfor: 
            if node not in visited: 
                if dfs(node, visited, rec_stack): 
                    return True
        
        return False
    
    def waiting(self, tid: int): 
        return tid in self.waitfor

    def __str__(self):
        return f"{self.waitfor}"

class ConcurrencyControlManager:
    def __init__(self, algorithm: str):
        self.algorithm = algorithm
        self.lock_S = {} # Map DataItem -> Set[transaction_id]
        self.lock_X = {} # Map DataItem -> transaction_id
        self.lock_IX = {} # Map DataItem -> transaction_id
        self.lock_IS = {} # Map DataItem -> transaction_id
        self.lock_SIX = {} # Map DataItem -> transaction_id
        self.wait_for_graph = WaitForGraph()
        self.transaction_queue = {} # Map transaction_id -> List[Row]
        self.waiting_list = [Transaction]
    
    def __str__(self):
        return f"===== ConcurrencyControlManager =====\nalgorithm: {self.algorithm}\n=====================================\n"
    
    def __generate_id(self) -> int:
        # return int(f"{datetime.now().strftime("%Y%m%d%H%M%S%f")}{random.randint(10000, 99999)}")
        return int((str(time.perf_counter()*1000000000)).replace('.', ''))
    
    def begin_transaction(self) -> int:
        # will return transaction_id: int
        transaction_id = self.__generate_id()
        self.transaction_queue[transaction_id] = []
        return transaction_id

    def log_object(self, object: Row, transaction_id: int):
        # implement lock on an object
        # assign timestamp on the object
        pass
    
    # check if lock is valid to current lock and its ancestor
    def check_lock_conflict(self, current, lock_type):
        conflict_matrix = {
            "IS": {"X"},
            "IX": {"S", "SIX", "X"},
            "S": {"IX", "SIX", "X"},
            "SIX": {"IX", "S", "SIX", "X"},
            "X": {"IS", "IX", "S", "SIX", "X"}
        }

        for held_lock, holders in [
            ("IS", self.lock_IS.get(current, set())),
            ("IX", self.lock_IX.get(current, set())),
            ("S", self.lock_S.get(current, set())),
            ("SIX", self.lock_SIX.get(current, set())),
            ("X", {self.lock_X.get(current)} if current in self.lock_X else set())
        ]:
            if holders and lock_type in conflict_matrix[held_lock]:
                return False, f"Conflict: Current {current} holds {held_lock} lock by another transaction."

        return True, None
            
    # Apply lock to a data item and update the ancestor
    def apply_lock_with_hierarchy(self, data_item, transaction_id, lock_type):
        current = data_item
        success = True
        message = None

        # Validate lock
        allowed, message = self.check_lock_conflict(current, lock_type)
        if not allowed:
            return Response(success=False, transaction_id=transaction_id, message=message)

        # Apply lock to parent
        current = data_item
        while current:
            parent = None
            if isinstance(current, Cell):
                parent = current.get_row()
            elif isinstance(current, Row):
                parent = current.get_table()

            # Update parent lock 
            if parent:
                if lock_type == "X":
                    if parent in self.lock_IS and transaction_id in self.lock_IS[parent]:
                        # Upgrade IS to SIX 
                        self.lock_IS[parent].remove(transaction_id)
                        if not self.lock_IS[parent]:
                            del self.lock_IS[parent]
                        self.lock_SIX.setdefault(parent, set()).add(transaction_id)
                    elif parent not in self.lock_SIX:  
                        self.lock_IX.setdefault(parent, set()).add(transaction_id)
                elif lock_type == "S":
                    self.lock_IS.setdefault(parent, set()).add(transaction_id)

            current = parent

        # apply lock to target
        if lock_type == "S":
            self.lock_S.setdefault(data_item, set()).add(transaction_id)
        elif lock_type == "X":
            self.lock_X[data_item] = transaction_id

        return Response(success=True, transaction_id=transaction_id, message=f"Lock-{lock_type} granted.")
    
    #  Validate and apply lock
    def validate_object(self, object: DataItem, transaction_id: int, action: str) -> Response:
        action = action.lower()
        if action not in ["read", "write"]:
            raise ValueError(f"Invalid action type: {action}. Allowed actions are 'read' or 'write'.")
        lock_type = 'S' if action == "read" else 'X'

        data_item = object.data_item

        # Apply lock with hierarchical checks
        response = self.apply_lock_with_hierarchy(data_item, transaction_id, lock_type)
        if not response.success:
            print(f"Failed to grant Lock-{lock_type} on {data_item} for transaction {transaction_id}: {response.message}")
            return response

        # If successful, return response
        print(f"Lock-{lock_type} granted on {data_item} for transaction {transaction_id}")
        return response

    # IF: transaction_id can be granted lock with type lock_type to data_item  
    # FS: transaction_id granted lock with type lock_type to  data_item
    def lock(self, data_item, transaction_id: int, lock_type: str): 
        if (lock_type == "X"): 
            self.lock_X[data_item] = transaction_id 
        else: # lock_type = "S"
            self.lock_S[data_item] = transaction_id   
    
    def end_transaction(self, transaction_id: int):
        # Flush objects of a particular transaction after it has successfully committed/aborted
        # Terminates the transaction
        for row in self.transaction_queue[transaction_id]:
            if row in self.lock_X and self.lock_X[row] == transaction_id:
                del self.lock_X[row]
            
            if row in self.lock_S and transaction_id in self.lock_S[row]:
                self.lock_S[row].remove(row)
                if self.lock_S[row] == []:
                    del self.lock_S[row]
        
        del self.transaction_queue[transaction_id]
        self.wait_for_graph.deleteNode(transaction_id)
        
    def process_waiting_list(self): 
        need_check = True
        while (need_check): 
            need_check = False
            for transaction in self.waiting_list: 
                if self.wait_for_graph.waiting(transaction.id): 
                    break
                
                # TODO: adjust lock type 
                lock_type = ""
                if (transaction.action == "WRITE"): 
                    lock_type = "X"
                else : 
                    lock_type = "S"

                self.lock(transaction.data_item, transaction.id, lock_type)
                #TODO: determine when to commit. This line might be the time that 
                # the transaction committed. Hence, the node in waitgraph might be removed    
            
                self.waiting_list.remove(transaction)

            
        
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