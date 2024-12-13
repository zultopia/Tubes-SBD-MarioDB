from datetime import datetime
from typing import Optional, List  # kalo gaboleh dihapus aja
import time
from ConcurrencyControlManager.utils import *

# Recovery Manager
from FailureRecoveryManager.FailureRecoveryManager import FailureRecoveryManager
from FailureRecoveryManager.RecoverCriteria import RecoverCriteria
from FailureRecoveryManager.Buffer import Buffer
            
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
        self.tid = 0
        self.action_log: List[TransactionAction] = []
        # self.failure_recovery = FailureRecoveryManager(Buffer(5),"./FailureRecoveryManager/log5.log")
    
    def __str__(self):
        return f"===== ConcurrencyControlManager =====\nalgorithm: {self.algorithm}\n=====================================\n"
    
    def __generate_id(self) -> int:
        # return int(f"{datetime.now().strftime("%Y%m%d%H%M%S%f")}{random.randint(10000, 99999)}")
        # return int((str(time.perf_counter()*1000000000)).replace('.', ''))
        self.tid += 1
        return self.tid
    
    def begin_transaction(self) -> int:
        # will return transaction_id: int
        transaction_id = self.__generate_id()
        self.transaction_dataitem_map[transaction_id] = {}
        self.log_object(TransactionAction(transaction_id, "start", None, None, None))
        return transaction_id

    def log_object(self, transactionAction: TransactionAction):
        # Recovery Manager
        # self.failure_recovery.write_log(transactionAction)
        self.action_log.insert(0, transactionAction)
    
    def rollback(self, transaction_ids: set):
        # Unlock
        for transaction_id in transaction_ids:
            for data_item, lock_type in self.transaction_dataitem_map[transaction_id].items(): 
                if (lock_type == "X"):
                    # print(f"Transaction {transaction_id} remove X on {data_item}")
                    self.lock_X[data_item].remove(transaction_id)
                if (lock_type == "S"): 
                    # print(f"Transaction {transaction_id} remove S on {data_item}")
                    # print("lock-S", self.lock_S)
                    self.lock_S[data_item].remove(transaction_id)
                if (lock_type == "IX"): 
                    # print(f"Transaction {transaction_id} remove IX on {data_item}")
                    self.lock_IX[data_item].remove(transaction_id)
                if (lock_type == "IS"): 
                    # print(f"Transaction {transaction_id} remove IS on {data_item}")
                    self.lock_IS[data_item].remove(transaction_id)
                if (lock_type == "SIX"): 
                    # print(f"Transaction {transaction_id} remove SIX on {data_item}")
                    self.lock_SIX[data_item].remove(transaction_id)
            
            self.transaction_dataitem_map[transaction_id] = {}
            # Recovery Manager
            # self.failure_recovery.recover(RecoverCriteria(None, transaction_id))
        
        rollback_t_actions = []
        for t_action in self.action_log:
            if(t_action.id in transaction_ids):
                # print(">>> ROLLED TID", t_action.id)
                rollback_t_actions.append(t_action)
                
        for t_act in rollback_t_actions:
            self.action_log.remove(t_act)
                
        return rollback_t_actions
        
    
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
            parent = current.get_parent()
            validate_stack.append(current)
            current = parent

        # Validate against the conflict matrix            
        while validate_stack:
            current = validate_stack.pop()
            abort = False
            failed = False
            
            conflict_list = []
            
            isAlreadyHasLock = False
            
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
                # print(self.transaction_dataitem_map)
                # print(transaction_id)
                # print("------------------------------ALAKAZAM")
                # print("MINIMUM LOCK TYPE:", minimum_lock_type)
                # print("DATAITEM MAP:", self.transaction_dataitem_map)
                if self.transaction_dataitem_map[transaction_id].get(current, None) == minimum_lock_type:
                    # print("AVARA KEDAVARA")
                    isAlreadyHasLock = True
                    break
                
                if holders and minimum_lock_type in conflict_matrix[held_lock]:
                    # print("##### lock-X:", self.lock_X)
                    copy_holders = list(holders)
                    if transaction_id in copy_holders: 
                        copy_holders.remove(transaction_id)
                    
                    conflict_list.extend(copy_holders)
                    
                    # print("#####1 lock-X:", self.lock_X)
                    if len(copy_holders) > 0:
                        failed = True
                        

                        for tid in copy_holders:
                            # print(">>>>", transaction_id, "WAIT_FOR", tid, "ON", minimum_lock_type, held_lock)
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
            if isAlreadyHasLock:
                continue
            abort_transaction_id = set()
            # print("#####2 lock-X:", self.lock_X)
            if failed:
                wait = False
                    
                for holder in conflict_list:
                    if transaction_id < holder:
                        abort_transaction_id.add(holder)
                    else:
                        wait = True
                        break
                if wait:  
                    self.transaction_queue.add(transaction_id)
                    return False, abort, f"Transaction {transaction_id} failed to get lock-{lock_type} on {transaction_action.data_item}"
                    
                # Wound
                for tid in abort_transaction_id: 
                    self.wait_for_graph.deleteNode(tid)
                    self.wait_for_graph.addEdge(tid, transaction_id)
                    self.transaction_queue.add(tid)
                # TODO: recovery, append to waiting_list
                undone_transaction = self.rollback(abort_transaction_id)
                for t_act in undone_transaction:
                    self.waiting_list.insert(0, t_act)
            
            # print("#####3 lock-X:", self.lock_X)
            current_lock = self.transaction_dataitem_map[transaction_id].get(current,None)
            if not current_lock:
                if minimum_lock_type == "IS":
                    # print(f"Transaction {transaction_id} granted lock-{minimum_lock_type} on {current}")
                    self.lock_IS.setdefault(current, set()).add(transaction_id)
                    # print("lock-IS bruhhhhhhh", self.lock_IS)
                elif minimum_lock_type == "IX":
                    # print(f"Transaction {transaction_id} granted lock-{minimum_lock_type} on {current}")
                    self.lock_IX.setdefault(current, set()).add(transaction_id)
                elif minimum_lock_type == "S":
                    # print(f"Transaction {transaction_id} granted lock-{minimum_lock_type} on {current}")
                    self.lock_S.setdefault(current, set()).add(transaction_id)
                elif minimum_lock_type == "X":
                    # print(f"Transaction {transaction_id} granted lock-{minimum_lock_type} on {current}")
                    self.lock_X.setdefault(current, set()).add(transaction_id)
                elif minimum_lock_type == "SIX":
                    # print(f"Transaction {transaction_id} granted lock-{minimum_lock_type} on {current}")
                    self.lock_SIX.setdefault(current, set()).add(transaction_id)
                   
            else:
                # print("#####4 lock-IS:", self.lock_IS)
                if current_lock not in minimum_lock_matrix[minimum_lock_type]:
                    # print("#####4 kedua lock-IS:", self.lock_IS)
                    # print("#####4 kedua lock-IS:", current_lock)
                    if current_lock == "S":
                        if current in self.lock_S:
                            # print("Discard S: ", transaction_id)
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
                    # print("#####5 lock-X:", self.lock_X)
                    if minimum_lock_type == "S":
                        # print(f"Transaction {transaction_id} granted lock-{minimum_lock_type} on {current}")
                        self.lock_S.setdefault(current, set()).add(transaction_id)
                    elif minimum_lock_type == "IX":
                        # print(f"Transaction {transaction_id} granted lock-{minimum_lock_type} on {current}")
                        self.lock_IX.setdefault(current, set()).add(transaction_id)
                    elif minimum_lock_type == "SIX":
                        # print(f"Transaction {transaction_id} granted lock-{minimum_lock_type} on {current}")
                        self.lock_SIX.setdefault(current, set()).add(transaction_id)
                    elif minimum_lock_type == "X":
                        # print(f"Transaction {transaction_id} granted lock-{minimum_lock_type} on {current}")
                        self.lock_X.setdefault(current, set()).add(transaction_id)
            # print("#####6 lock-X:", self.lock_X)
            if not current_lock or current_lock not in minimum_lock_matrix[minimum_lock_type]:        
                self.transaction_dataitem_map[transaction_id][current] = minimum_lock_type
                # print("dataitem map:", self.transaction_dataitem_map)
        
        # print("=========================================")
        # print("lock-IS:", self.lock_IS)
        # print("lock-IX:", self.lock_IX)
        # print("lock-S:", self.lock_S)
        # print("lock-X:", self.lock_X)
        # print("lock-SIX:", self.lock_SIX)
        # print("dataitem map:", self.transaction_dataitem_map)
        
        return True, False, f"Transaction {transaction_id} successfully get lock-{lock_type} on {transaction_action.data_item}"
    
    #  Validate and apply lock
    def validate_object(self, transactionAction: TransactionAction, isProcessWaitingList: bool=False) -> Response:
        transaction_id = transactionAction.id
        action = transactionAction.action
        
        if(transactionAction.action == "start"):
            self.log_object(transactionAction)
            return Response(True, "success", transaction_id, "Transaction Restarted")
        
        if transactionAction.id in self.transaction_queue:
            # print("append3: ", transactionAction.id)
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
            # print(f"Failed to grant Lock-{lock_type} on {transactionAction.data_item} for transaction {transaction_id}: {message}")
            if not abort:
                # print("append1: ", transactionAction.id)
                if(not isProcessWaitingList):
                    self.waiting_list.append(transactionAction)
            # else:
            #     self.end_transaction(transactionAction.id, "abort")
            
            return Response(allowed, "wait" if not abort else "abort", transaction_id, message)
        if(transactionAction.action == "write"):
            self.log_object(transactionAction)
        return Response(allowed, "success", transaction_id, message)
    
    def end_transaction(self, transaction_id: int, status: str):
        # print("a" * 50)
        # print("dataitem map:", self.transaction_dataitem_map[transaction_id])
        # print("lock-IS:", self.lock_IS)
        # print("lock-S:", self.lock_S)
        # print("lock-IX:", self.lock_IX)
        # print("lock-X:", self.lock_X)
        # print("lock-SIX:", self.lock_SIX)
        # print("a" * 50)
        
        for data_item, lock_type in self.transaction_dataitem_map[transaction_id].items(): 
            if (lock_type == "X"):
                # print(f"Transaction {transaction_id} remove X on {data_item}")
                self.lock_X[data_item].remove(transaction_id)
            if (lock_type == "S"): 
                # print(f"Transaction {transaction_id} remove S on {data_item}")
                # print("lock-S", self.lock_S)
                self.lock_S[data_item].remove(transaction_id)
            if (lock_type == "IX"): 
                # print(f"Transaction {transaction_id} remove IX on {data_item}")
                self.lock_IX[data_item].remove(transaction_id)
            if (lock_type == "IS"): 
                # print(f"Transaction {transaction_id} remove IS on {data_item}")
                self.lock_IS[data_item].remove(transaction_id)
            if (lock_type == "SIX"): 
                # print(f"Transaction {transaction_id} remove SIX on {data_item}")
                self.lock_SIX[data_item].remove(transaction_id)
        
        if transaction_id in self.transaction_queue:
            self.transaction_queue.remove(transaction_id)
        self.wait_for_graph.deleteNode(transaction_id)
        del self.transaction_dataitem_map[transaction_id]
        
        t_action = TransactionAction(transaction_id, status, None, None, None)
        self.log_object(t_action)
        
        self.process_waiting_list()
        
    def process_waiting_list(self): 
        check_waiting = False
        run_transaction = []

        # print("transaction queue", self.transaction_queue)
        # print(self.wait_for_graph.waitfor)
        for tid in self.transaction_queue: 
            if not self.wait_for_graph.waiting(tid):
                run_transaction.append(tid)
                check_waiting = True

        for transaction_id in run_transaction:
            self.transaction_queue.remove(transaction_id)

        # print("check_waiting", check_waiting)
        
        # if check_waiting:
        index_insert = 0
        for index, t_process in enumerate(list(self.waiting_list)):
            # print(">>>>> PROSES WAITING LIST, IDX", index)
            if t_process.id not in self.transaction_queue :
                # print(">>>>>> TDI", t_process.id, "NOT IN QUEUE")
                self.waiting_list.remove(t_process)
                # print(">>>>>>>>>DATAITEM MAP BEFORE", self.transaction_dataitem_map)
                response = self.validate_object(t_process, True)
                # print(">>>>>>>>>DATAITEM MAP AFTER", self.transaction_dataitem_map)
                if not response.allowed:
                    # print(">>>>>>>>>TDI", t_process.id, t_process.action, "NOT ALLOWED")
                    self.waiting_list.insert(index_insert, t_process)
                    index_insert += 1
                    # self.transaction_queue.add(response.transaction_id)
                # print(">>>>>> QUEUE", self.transaction_queue)
                    
                if (response.status == "abort" or response.status == "commit"):
                    # print(">>>>> COMMIT OR ABORT idx", index, "TID", t_process.id)
                    break