import json
from FailureRecoveryManager.RecoverCriteria import RecoverCriteria
from FailureRecoveryManager.ExecutionResult import ExecutionResult
from datetime import datetime
from FailureRecoveryManager.Rows import Rows

class FailureRecoveryManager:
    def __init__(self, log_file="./FailureRecoveryManager/log.log"):
        self._max_size_log = 20 #sesuaikan lagi
        self._max_size_buffer = 20 #sesuaikan lagi
        self._log_file = log_file
        self._wh_logs = ["102|2024-11-21T12:02:05.678Z|IN_PROGRESS|INSERT INTO employees (id, name, salary) VALUES (2, 'Bob', 4000);|Before: None|After: [{\"id\": 2, \"name\": \"Bob\", \"salary\": 4000}]","102|2024-11-21T12:02:20.123Z|ROLLBACK|DELETE FROM employees WHERE id = 2;|Before: None|After: None","102|2024-11-21T12:02:15.789Z|ABORTED|TRANSACTION END|[{}]","103|2024-11-21T12:02:45.321Z|IN_PROGRESS|UPDATE employees SET salary = 6000 WHERE id = 1;|Before: [{\"id\": 1, \"name\": \"Alice\", \"salary\": 5000}]|After: [{\"id\": 1, \"name\": \"Alice\", \"salary\": 6000}]"]
        self._buffer = []
        # "CHECKPOINT|2024-11-21T12:03:00.000Z|[103]"
        
    def get_buffer(self):
        return self._buffer()
    
    def set_buffer(self, buffer):
        self._buffer = buffer
    
    def add_buffer(self, data):
        self._buffer.append(data)
    
    def clear_buffer(self):
        self._buffer.clear()
        
    def remove_buffer(self, data):
        self._buffer.remove(data)
                       
    def write_log(self, execution_result: ExecutionResult) -> None:
        # This method accepts execution result object as input and appends 
        # an entry in a write-ahead log based on execution info object. 

        # Create log entry
        def process_rows(rows):
            return ", ".join([str(item) for item in rows]) 

        log_entry = (
            f"{execution_result.transaction_id}|"
            f"{execution_result.timestamp}|"
            f"{execution_result.status}|"
            f"{execution_result.message}|"
            f"{execution_result.query}|"
            f"Before: {process_rows(execution_result.data_before.data) if execution_result.data_before.data else None}|"
            f"After: {process_rows(execution_result.data_after.data) if execution_result.data_after.data else None}"
        )   # execution_result.message ga kepake (?)

        # Append entry to self._wh_logs
        self._wh_logs.append(log_entry)
    
    def _save_checkpoint():
        pass
    
    def _read_lines_from_end(self, file_path, chunk_size=1024):
        
        with open(file_path, "rb") as file:
            file.seek(0, 2)  
            file_size = file.tell()
            buffer = b""
            position = file_size

            while position > 0:
                to_read = min(chunk_size, position) 
                position -= to_read
                file.seek(position)  
                chunk = file.read(to_read)
                buffer = chunk + buffer  

                lines = buffer.split(b"\n")
                
                buffer = lines.pop(0) if position > 0 else b""

                for line in reversed(lines):
                    yield line.decode("utf-8")
            if buffer:
                yield buffer.decode("utf-8")
                
    def recover(self, criteria: RecoverCriteria = None):
        recovered_transactions = []
        active_transactions = set()
        if criteria and criteria.timestamp:
            checkpoint_found = False
            #baca whl
            for log_line in self._wh_logs:
                log_parts = log_line.split("|")
                
                if log_parts[0] == "CHECKPOINT":
                    active_transactions = set(json.loads(log_parts[2]))
                    checkpoint_found = True
                    break
                elif log_parts[0].isdigit():
                    timestamp = log_parts[1]
                    date_timestamp = datetime.fromisoformat(timestamp)
                    date_criteria_timestamp = datetime.fromisoformat(criteria.timestamp)
                    
                    if date_timestamp < date_criteria_timestamp:
                        break
                    
                    recovered_transactions.insert(0, log_line)
            
            #baca log file
            if not checkpoint_found:
                for log_line in self._read_lines_from_end(self._log_file):
                    log_parts = log_line.split("|")

                    if log_parts[0] == "CHECKPOINT":
                        active_transactions = set(json.loads(log_parts[2]))
                        break
                    elif log_parts[0].isdigit():
                        timestamp = log_parts[1]
                        date_timestamp = datetime.fromisoformat(timestamp)
                        date_criteria_timestamp = datetime.fromisoformat(criteria.timestamp)

                        if date_timestamp < date_criteria_timestamp:
                            break
                        
                        recovered_transactions.insert(0, log_line)
                    
            
        elif criteria and criteria.transaction_id:
            checkpoint_found = False
            for log_line in self._wh_logs:
                log_parts = log_line.split("|")
                
                if log_parts[0] == "CHECKPOINT":
                    active_transactions = set(json.loads(log_parts[2]))
                    checkpoint_found = True
                    
                    break
                
                elif log_parts[0].isdigit():
                    transaction_id = log_parts[0]
                    
                    if int(transaction_id) < criteria.transaction_id:
                        break
                    
                    recovered_transactions.insert(0, log_line)
            
            #baca log file
            if not checkpoint_found:
                for log_line in self._read_lines_from_end(self._log_file):
                    log_parts = log_line.split("|")
                    
                    if log_parts[0] == "CHECKPOINT":
                        active_transactions = set(json.loads(log_parts[2]))
                        break
                    
                    elif log_parts[0].isdigit():
                        transaction_id = log_parts[0]
                        
                        if int(transaction_id) < criteria.transaction_id:
                            break
                        
                        recovered_transactions.insert(0, log_line)
                    
        #redo
        print("recover tx: ", recovered_transactions)
        for recovered_transaction in recovered_transactions:
            log_parts = recovered_transaction.split("|")
            transaction_id = log_parts[0]
            status = log_parts[2]
            
            if status == "COMMITTED":
                active_transactions.discard(transaction_id)
            elif status == "ABORTED":
                active_transactions.discard(transaction_id)
            elif status == "START":
                active_transactions.add(transaction_id)
            elif status == "IN_PROGRESS" or status == "ROLLBACK":
                #memanggil query processor untuk menjalankan ulang query (redo)
                print("send query: ", transaction_id, " ", log_parts[3])
        
        active_transactions = sorted(active_transactions)
        print("active tx: ", active_transactions)
        
        #undo
        for tx in active_transactions:
            log_current_tx =  list(filter(lambda x: int(x.split("|")[0]) == tx and x.split("|")[2] == "IN_PROGRESS", recovered_transactions))
            print("current tx: ", log_current_tx)
            
            for curr in log_current_tx:
                log_parts = curr.split("|")
                sql_operation = log_parts[3]
                try:
                    before_states_raw = log_parts[4].split("Before: ")[1].strip()
                    after_states_raw = log_parts[5].split("After: ")[1].strip()

                    if before_states_raw == "None":
                        before_states = None
                    else:
                        before_states = json.loads(before_states_raw)

                    if after_states_raw == "None":
                        after_states = None
                    else:
                        after_states = json.loads(after_states_raw)

                    

                except IndexError:
                    print("Error: Missing 'Before' or 'After' in log_parts.")
                    exit()
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")
                    exit()

                rollback_queries = []

                if sql_operation.startswith("INSERT INTO"):
                    table = sql_operation.split(" ")[2]
                    for after_state in after_states:  
                        where_clause = " AND ".join([f"{k} = {repr(v)}" for k, v in after_state.items()])
                        rollback_query = f"DELETE FROM {table} WHERE {where_clause};"
                        rollback_queries.append(rollback_query)

                elif sql_operation.startswith("DELETE FROM"):
                    table = sql_operation.split(" ")[2]
                    for before_state in before_states:  
                        columns = ", ".join(before_state.keys())
                        values = ", ".join([repr(v) for v in before_state.values()])
                        rollback_query = f"INSERT INTO {table} ({columns}) VALUES ({values});"
                        rollback_queries.append(rollback_query)

                elif sql_operation.startswith("UPDATE"):
                    table = sql_operation.split(" ")[1]
                    for before_state, after_state in zip(before_states, after_states): 
                        set_clause = ", ".join([f"{k} = {repr(v)}" for k, v in before_state.items()])
                        where_clause = " AND ".join([f"{k} = {repr(v)}" for k, v in after_state.items()])
                        rollback_query = f"UPDATE {table} SET {set_clause} WHERE {where_clause};"
                        rollback_queries.append(rollback_query)

                print("rollback query: ", rollback_queries)
                for rollback_query in rollback_queries:
                    print("query rollback: ", rollback_query)
                    res = None 

                    exec_result = ExecutionResult(
                        tx, datetime.now().isoformat(), "TRANSACTION ROLLBACK",
                        Rows(None), Rows(res), "ROLLBACK", rollback_query
                    )
                    print("write log: ", exec_result.__dict__)
                    self.write_log(exec_result)
            
            exec_result = ExecutionResult(tx, datetime.now().isoformat(), "TRANSACTION END", Rows(None), Rows(None), "ABORT", None)
            print("write log: ", exec_result.__dict__)
            
            self.write_log(exec_result)
            
            
    def recover_system_crash(self):
        recovered_transactions = []
        active_transactions = set()

        for log_line in self._read_lines_from_end(self._log_file):
            log_parts = log_line.split("|")
            
            if log_parts[0] == "CHECKPOINT":
                active_transactions = set(json.loads(log_parts[2]))
                break
            elif log_parts[0].isdigit():
                recovered_transactions.insert(0, log_line)
        
        #redo
        for recovered_transaction in recovered_transactions:
            log_parts = recovered_transaction.split("|")
            transaction_id = log_parts[0]
            status = log_parts[2]
            
            if status == "COMMITTED":
                active_transactions.discard(transaction_id)
            elif status == "ABORTED":
                active_transactions.discard(transaction_id)
            elif status == "START":
                active_transactions.add(transaction_id)
            elif status == "IN_PROGRESS" or status == "ROLLBACK":
                #memanggil query processor untuk menjalankan ulang query (redo)
                pass
        
        active_transactions = sorted(active_transactions)
        
        #undo
        for tx in active_transactions:
            log_current_tx =  list(filter(lambda x: int(x.split("|")[0]) == tx and x.split("|")[2] == "IN_PROGRESS", recovered_transactions))
            print("current tx: ", log_current_tx)
            
            for curr in log_current_tx:
                log_parts = curr.split("|")
                sql_operation = log_parts[3]
                try:
                    before_states_raw = log_parts[4].split("Before: ")[1].strip()
                    after_states_raw = log_parts[5].split("After: ")[1].strip()

                    if before_states_raw == "None":
                        before_states = None
                    else:
                        before_states = json.loads(before_states_raw)

                    if after_states_raw == "None":
                        after_states = None
                    else:
                        after_states = json.loads(after_states_raw)

                    

                except IndexError:
                    print("Error: Missing 'Before' or 'After' in log_parts.")
                    exit()
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")
                    exit()

                rollback_queries = []

                if sql_operation.startswith("INSERT INTO"):
                    table = sql_operation.split(" ")[2]
                    for after_state in after_states:  
                        where_clause = " AND ".join([f"{k} = {repr(v)}" for k, v in after_state.items()])
                        rollback_query = f"DELETE FROM {table} WHERE {where_clause};"
                        rollback_queries.append(rollback_query)

                elif sql_operation.startswith("DELETE FROM"):
                    table = sql_operation.split(" ")[2]
                    for before_state in before_states:  
                        columns = ", ".join(before_state.keys())
                        values = ", ".join([repr(v) for v in before_state.values()])
                        rollback_query = f"INSERT INTO {table} ({columns}) VALUES ({values});"
                        rollback_queries.append(rollback_query)

                elif sql_operation.startswith("UPDATE"):
                    table = sql_operation.split(" ")[1]
                    for before_state, after_state in zip(before_states, after_states): 
                        set_clause = ", ".join([f"{k} = {repr(v)}" for k, v in before_state.items()])
                        where_clause = " AND ".join([f"{k} = {repr(v)}" for k, v in after_state.items()])
                        rollback_query = f"UPDATE {table} SET {set_clause} WHERE {where_clause};"
                        rollback_queries.append(rollback_query)

                print("rollback query: ", rollback_queries)
                for rollback_query in rollback_queries:
                    print("query rollback: ", rollback_query)
                    res = None 

                    exec_result = ExecutionResult(
                        tx, datetime.now().isoformat(), "TRANSACTION ROLLBACK",
                        Rows(None), Rows(res), "ROLLBACK", rollback_query
                    )
                    print("write log: ", exec_result.__dict__)
                    self.write_log(exec_result)
            
            exec_result = ExecutionResult(tx, datetime.now().isoformat(), "TRANSACTION END", Rows(None), Rows(None), "ABORT", None)
            print("write log: ", exec_result.__dict__)
            
            self.write_log(exec_result)