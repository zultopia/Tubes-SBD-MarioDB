from datetime import datetime
from typing import Optional, List  # kalo gaboleh dihapus aja
import time

# from FailureRecoveryManager.FailureRecoveryManager import FailureRecoveryManager

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
    
    def __hash__(self):
        concatted_keys = ""
        for key in self.keys:
            concatted_keys += str(key)
        return hash(concatted_keys)

    def __str__(self):
        return (
            f"===== PrimaryKey =====\nisComposite: {self.isComposite}\nkeys: {self.keys}\n======================\n"
        )

class IDataItem:
    def get_parent(self):
        pass
    
class Database(IDataItem):
    def get_parent(self):
        return None

class Table(IDataItem):
    def __init__(self, table: str):
        self.table = table
    
    def __eq__(self, other: 'Table'):
        if isinstance(other, Table):
            return self.table == other.table
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.table)
    
    def __str__(self):
        return self.table
    
class Row(IDataItem):
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
        if isinstance(other, Row):
            return (self.pkey == other.pkey) and (self.table == other.table)
        return False

    def __ne__(self, other):
        return not self.__eq__(other)
    
    def __str__(self):
        return f"=============== Row ===============\ntable: {self.table}\nmap:\n{self.map}\npkey:\n{self.pkey}===================================\n"

    def __hash__(self):
        return hash(self.table.__str__() + self.pkey.__str__())
    
    def get_table(self): 
        return self.table
    
    def get_parent(self):
        return self.get_table()

class Cell(IDataItem):
    def __init__(self, table: Table, row: Row, pkey: PrimaryKey, attribute: str, value: any):
        self.row = row
        self.table = table
        self.pkey = pkey
        self.attribute = attribute
        self.value = value
    
    def __eq__(self, other):
        if isinstance(other, Cell):
            return (self.pkey == other.pkey) and (self.attribute == other.attribute) and (self.table == other.table)
        return False

    def __hash__(self):
        return hash(self.table + self.pkey.__str__() + self.attribute)
    
    def get_row(self):
        return self.row
    
    def get_table(self):
        return self.row.get_table()

    def get_parent(self):
        return self.get_row()

class DataItem:
    def __init__(self, level: str, data_item):
        self.level = level
        self.data_item = data_item
        
    def __eq__(self, other):
        if isinstance(other, DataItem):
            return (self.level == other.level) and (self.data_item == other.data_item)
        return False
    
    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return self.data_item.__hash__()
    
    def get_parent(self):
        return self.data_item.get_parent()

class Action:
    def __init__(self, action : str):
        self.action = action 
        
    def __str__(self):
        return f"=== Action ===\naction: {self.action}\n==============\n"
        
class Response:
    def __init__(self, allowed: bool, status: str, transaction_id: int, message: str = ""):
        self.allowed = allowed
        self.status = status
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


class TransactionAction: 
    def __init__(self, tid: int, action: Action, level: str, data_item: DataItem, old_data_item: DataItem = None): 
        self.id = tid 
        self.action = action
        self.level  = level
        self.data_item = data_item
        self.old_data_item = old_data_item
    
    def __eq__(self, other):
        if isinstance(other, TransactionAction):
            return (self.id == other.id) and (self.action == other.action) and (self.level == other.level) and (self.data_item == other.data_item) and (self.old_data_item == other.old_data_item)
        return False

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
        self.lock_IX = {} # Map DataItem -> Set[transaction_id]
        self.lock_IS = {} # Map DataItem -> Set[transaction_id]
        self.lock_SIX = {} # Map DataItem -> transaction_id
        self.wait_for_graph = WaitForGraph()
        self.transaction_queue = set() # Set transaction_id
        self.transaction_dataitem_map = {} # Map transaction_id -> List[DataItem]
        self.waiting_list : List[TransactionAction] = []
        # self.failure_recovery = FailureRecoveryManager()
    
    def __str__(self):
        return f"===== ConcurrencyControlManager =====\nalgorithm: {self.algorithm}\n=====================================\n"
    
    def __generate_id(self) -> int:
        # return int(f"{datetime.now().strftime("%Y%m%d%H%M%S%f")}{random.randint(10000, 99999)}")
        return int((str(time.perf_counter()*1000000000)).replace('.', ''))
    
    def begin_transaction(self) -> int:
        # will return transaction_id: int
        transaction_id = self.__generate_id()
        self.transaction_dataitem_map[transaction_id] = {}
        return transaction_id

    def log_object(self, transactionAction: TransactionAction):
        # self.failure_recovery.write_log(transactionAction)
        pass
    
    # check if lock is valid to current lock and its ancestor
    def apply_lock(self, transaction_action: TransactionAction):
        current = transaction_action.data_item
        lock_type = "S" if transaction_action.action.lower() == "read" else "X" if transaction_action.action.lower() == "write" else "SIX"
        transaction_id = transaction_action.id
        
        conflict_matrix = {
            "IS": {"X"},
            "IX": {"S", "SIX", "X"},
            "S": {"IX", "SIX", "X"},
            "SIX": {"IX", "S", "SIX", "X"},
            "X": {"IS", "IX", "S", "SIX", "X"}
        }
        
        minimum_lock_matrix = {
            "IS": {"IX", "S", "SIX", "X"},
            "S": {"SIX", "X", "IX"},
            "IX": {"X", "SIX"},
            "SIX": {"X"},
            "X": {}
        }

        # Build the hierarchy stack
        validate_stack = []

        while current :
            if isinstance(current, Cell):
                parent = current.get_row()
            elif isinstance(current, Row):
                parent = current.get_table()
            else:
                parent = None

            validate_stack.append(current)
            current = parent

        # Validate against the conflict matrix            
        while validate_stack:
            current = validate_stack.pop()
            abort = False
            failed = False
            
            conflict_list = []
            
            for held_lock, holders in [
                ("IS", self.lock_IS.get(current, set())),
                ("IX", self.lock_IX.get(current, set())),
                ("S", self.lock_S.get(current, set())),
                ("SIX", self.lock_SIX.get(current, set())),
                ("X", self.lock_X.get(current, set())),
            ]:

                minimum_lock_type = lock_type
                if len(validate_stack) > 0:
                    if lock_type == "S":
                        minimum_lock_type = "IS"
                    elif lock_type == "X":
                        minimum_lock_type = "IX"
                print(self.transaction_dataitem_map)
                print(transaction_id)
                if self.transaction_dataitem_map[transaction_id].get(current, None) == minimum_lock_type:
                    break
                
                if holders and minimum_lock_type in conflict_matrix[held_lock]:
                    if transaction_id in holders:
                        holders.remove(transaction_id)
                        
                    if transaction_id not in holders:
                        print("tid", transaction_id)
                        print("holder", holders)
                        print("key", minimum_lock_type)
                        failed = True
                        conflict_list.append(holders)
                        
                        for tid in holders:
                            if held_lock == "S":
                                self.wait_for_graph.addEdge(transaction_id, tid)
                            if held_lock == "IS":
                                self.wait_for_graph.addEdge(transaction_id, tid)
                            if held_lock == "S":
                                self.wait_for_graph.addEdge(transaction_id, tid)
                            if held_lock == "S":
                                self.wait_for_graph.addEdge(transaction_id, tid)
                            if held_lock == "S":
                                self.wait_for_graph.addEdge(transaction_id, tid)
            
            abort_transaction_id = []
            if failed:
                for holders in conflict_list:
                    for other_transaction_id in holders:
                        # Wait-die
                        if other_transaction_id > transaction_id:
                            abort = True
                        # Wound-wait
                        # if transaction_id > other_transaction_id:
                        #     abort_transaction_id.append(other_transaction_id)
                        #     self.end_transaction(other_transaction_id, 'abort')
                        #     self.transaction_queue.add(transaction_id)
                        #     self.wait_for_graph.addEdge(other_transaction_id, transaction_id)
                        #     # TODO: recovery, append to waiting_list
                            
                if not abort:
                    self.transaction_queue.add(transaction_id)
                    print("append2: ", transaction_action.id)
                    self.waiting_list.append(transaction_action)
                    # self.wait_for_graph.addEdge()
                
                # while (not self.wait_for_graph.waiting(transaction_action)): 
                #     TODO: threading
                return False, abort, f"Transaction {transaction_id} failed to get lock-{lock_type} on {transaction_action.data_item}"
            
            
            current_lock = self.transaction_dataitem_map[transaction_id].get(current,None)
            if not current_lock:
                if minimum_lock_type == "IS":
                    print(f"Transaction {transaction_id} granted lock-{minimum_lock_type} on {current}")
                    self.lock_IS.setdefault(current, set()).add(transaction_id)
                elif minimum_lock_type == "IX":
                    print(f"Transaction {transaction_id} granted lock-{minimum_lock_type} on {current}")
                    self.lock_IX.setdefault(current, set()).add(transaction_id)
                elif minimum_lock_type == "S":
                    print(f"Transaction {transaction_id} granted lock-{minimum_lock_type} on {current}")
                    self.lock_S.setdefault(current, set()).add(transaction_id)
                elif minimum_lock_type == "X":
                    print(f"Transaction {transaction_id} granted lock-{minimum_lock_type} on {current}")
                    self.lock_X.setdefault(current, set()).add(transaction_id)
                elif minimum_lock_type == "SIX":
                    print(f"Transaction {transaction_id} granted lock-{minimum_lock_type} on {current}")
                    self.lock_SIX.setdefault(current, set()).add(transaction_id)
                   
            else:
                if current_lock not in minimum_lock_matrix[minimum_lock_type]:
                    if current_lock == "S":
                        if current in self.lock_S:
                            print("Discard S: ", transaction_id)
                            self.lock_S[current].discard(transaction_id)
                    elif current_lock == "IS":
                        if current in self.lock_IS:
                            self.lock_IS[current].discard(transaction_id)
                    elif current_lock == "IX":
                        if current in self.lock_IX:
                            self.lock_IX[current].discard(transaction_id)
                    elif current_lock == "SIX":
                        if current in self.lock_SIX:
                            self.lock_SIX[current].discard(transaction_id)

                    if minimum_lock_type == "S":
                        print(f"Transaction {transaction_id} granted lock-{minimum_lock_type} on {current}")
                        self.lock_S.setdefault(current, set()).add(transaction_id)
                    elif minimum_lock_type == "IX":
                        print(f"Transaction {transaction_id} granted lock-{minimum_lock_type} on {current}")
                        self.lock_IX.setdefault(current, set()).add(transaction_id)
                    elif minimum_lock_type == "SIX":
                        print(f"Transaction {transaction_id} granted lock-{minimum_lock_type} on {current}")
                        self.lock_SIX.setdefault(current, set()).add(transaction_id)
                    elif minimum_lock_type == "X":
                        print(f"Transaction {transaction_id} granted lock-{minimum_lock_type} on {current}")
                        self.lock_X.setdefault(current, set()).add(transaction_id)
                
            self.log_object(transaction_action)
            if not current_lock or current_lock not in minimum_lock_matrix[minimum_lock_type]:        
                self.transaction_dataitem_map[transaction_id][current] = minimum_lock_type
                print("dataitem map:", self.transaction_dataitem_map)
        
        print("=========================================")
        print("lock-IS:", self.lock_IS)
        print("lock-IX:", self.lock_IX)
        print("lock-S:", self.lock_S)
        print("lock-X:", self.lock_X)
        print("lock-SIX:", self.lock_SIX)
        print("dataitem map:", self.transaction_dataitem_map)
        
        return True, None, f"Transaction {transaction_id} successfully get lock-{lock_type} on {transaction_action.data_item}"
    
    #  Validate and apply lock
    def validate_object(self, transactionAction: TransactionAction) -> Response:
        transaction_id = transactionAction.id
        action = transactionAction.action
        
        if transactionAction.id in self.transaction_queue:
            print("append3: ", transactionAction.id)
            self.waiting_list.append(transactionAction)
            return Response(False, "wait", transactionAction.id, f"Transaction {transaction_id} must wait for {transactionAction.action}")
        
        action = action.lower()
        if action not in ["read", "write", "six", "commit", "abort"]:
            raise ValueError(f"Invalid action type: {action}. Allowed actions are 'read', 'write', 'six', 'commit', 'abort'.")
        
        if action in ["commit", "abort"]:
            self.end_transaction(transactionAction.id, action)
            return Response(True, action, transactionAction.id, f"Transaction {transaction_id} succesfully {action}ed")

        lock_type = 'S' if action == "read" else 'X' if action == "write" else "SIX"
        
        allowed, abort, message = self.apply_lock(transactionAction)
        if not allowed:
            print(f"Failed to grant Lock-{lock_type} on {transactionAction.data_item} for transaction {transaction_id}: {message}")
            if not abort:
                print("append1: ", transactionAction.id)
                self.waiting_list.append(transactionAction)
            else:
                self.end_transaction(transactionAction.id, "abort")
            
            return Response(allowed, "wait" if not abort else "abort", transaction_id, message)

        return Response(allowed, "success", transaction_id, message)
    
    def end_transaction(self, transaction_id: int, status: str):
        print("a" * 50)
        print("dataitem map:", self.transaction_dataitem_map[transaction_id])
        print("lock-IS:", self.lock_IS)
        print("lock-S:", self.lock_S)
        print("lock-IX:", self.lock_IX)
        print("lock-X:", self.lock_X)
        print("lock-SIX:", self.lock_SIX)
        print("a" * 50)
        
        for data_item, lock_type in self.transaction_dataitem_map[transaction_id].items(): 
            if (lock_type == "X"):
                print(f"Transaction {transaction_id} remove X on {data_item}")
                self.lock_X[data_item].remove(transaction_id)
            if (lock_type == "S"): 
                print(f"Transaction {transaction_id} remove S on {data_item}")
                print("lock-S", self.lock_S)
                self.lock_S[data_item].remove(transaction_id)
            if (lock_type == "IX"): 
                print(f"Transaction {transaction_id} remove IX on {data_item}")
                self.lock_IX[data_item].remove(transaction_id)
            if (lock_type == "IS"): 
                print(f"Transaction {transaction_id} remove IS on {data_item}")
                self.lock_IS[data_item].remove(transaction_id)
            if (lock_type == "SIX"): 
                print(f"Transaction {transaction_id} remove SIX on {data_item}")
                self.lock_SIX[data_item].remove(transaction_id)
        
        if transaction_id in self.transaction_queue:
            self.transaction_queue.remove(transaction_id)
        if self.wait_for_graph.waiting(transaction_id):
            self.wait_for_graph.deleteNode(transaction_id)
        del self.transaction_dataitem_map[transaction_id]
        
        t_action = TransactionAction(transaction_id, status, None, None, None)
        self.log_object(t_action)
        
        self.process_waiting_list()
        
    def process_waiting_list(self): 
        need_check = True

        while need_check: 
            check_waiting = False
            run_transaction = []

            print(self.wait_for_graph.waitfor)
            for tid in self.transaction_queue: 
                if not self.wait_for_graph.waiting(tid):
                    run_transaction.append(tid)
                    check_waiting = True

            for transaction_id in run_transaction:
                self.transaction_queue.remove(transaction_id)

            print(self.transaction_queue)
            for act in self.waiting_list: 
                print("ini", act.id)
            need_check = False
            if check_waiting:
                for t_process in list(self.waiting_list): 
                    if t_process.id not in self.transaction_queue : 
                        response = self.validate_object(t_process)
                        if response.status == "abort" or response.status == "commit": 
                            need_check = True
                
                        print("before remove")
                        for t_action in self.waiting_list:
                            print(t_action.id)
                        self.waiting_list.remove(t_action)
                        print("hasil remove", self.waiting_list)
                        for t_action in self.waiting_list:
                            print(t_action.id)
