import json
from RecoverCriteria import RecoverCriteria
from ExecutionResult import ExecutionResult
from datetime import datetime

class FailureRecoveryManager:
    def __init__(self, log_file="log.log"):
        self._max_size_log = 20 #sesuaikan lagi
        self._max_size_buffer = 20 #sesuaikan lagi
        self._log_file = log_file
        self._wh_logs = ["103|2024-11-21T12:02:45.321Z|IN_PROGRESS|UPDATE employees SET salary = 6000 WHERE id = 1;|Before: {'id': 1, 'name': 'Alice', 'salary': 5000}|After: {'id': 1, 'name': 'Alice', 'salary': 6000}", "CHECKPOINT|2024-11-21T12:03:00.000Z|[103]"]
        self._buffer = []
        
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
            f"{execution_result.query}|"
            f"Before: {process_rows(execution_result.data_before.data)}|"
            f"After: {process_rows(execution_result.data_after.data)}"
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
            log_current_tx =  list(filter(lambda x: recovered_transactions.split("|")[0] == tx and recovered_transactions.split("|")[2] == "IN_PROGRESS"))
            
            for curr in log_current_tx: 
                log_parts = curr.split("|")
                sql_operation = log_parts[3]
                before_state = json.loads(log_parts[4].split("Before: ")[1])
                after_state = json.loads(log_parts[5].split("After: ")[1])
                
                rollback_query = ""
                
                if sql_operation.startswith("INSERT INTO"):
                    table = sql_operation.split(" ")[2]
                    where_clause = " AND ".join([f"{k} = {repr(v)}" for k, v in after_state.items()])
                    rollback_query = f"DELETE FROM {table} WHERE {where_clause};"
                    
                elif sql_operation.startswith("DELETE FROM"):
                    table = sql_operation.split(" ")[2]
                    columns = ", ".join(before_state.keys())
                    values = ", ".join([repr(v) for v in before_state.values()])
                    rollback_query = f"INSERT INTO {table} ({columns}) VALUES ({values});"
                    
                elif sql_operation.startswith("UPDATE"):
                    table = sql_operation.split(" ")[1]
                    set_clause = ", ".join([f"{k} = {repr(v)}" for k, v in before_state.items()])
                    where_clause = " AND ".join([f"{k} = {repr(v)}" for k, v in after_state.items()])
                    rollback_query = f"UPDATE {table} SET {set_clause} WHERE {where_clause};"
                
                #memanggil query processor untuk menjalankan rollback query (undo)
                res = None #result dari query processor
                
                #write log rollback query?
                exec_result = ExecutionResult(tx, datetime.now().isoformat(), "TRANSACTION ROLLBACK", None, res, "ROLLBACK", rollback_query)
                self.write_log(exec_result)
            
            exec_result = ExecutionResult(tx, datetime.now().isoformat(), "TRANSACTION END", None, None, "ABORT", None)
            
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
            log_current_tx =  list(filter(lambda x: recovered_transactions.split("|")[0] == tx and recovered_transactions.split("|")[2] == "IN_PROGRESS"))
            
            for curr in log_current_tx: 
                log_parts = curr.split("|")
                sql_operation = log_parts[3]
                before_state = json.loads(log_parts[4].split("Before: ")[1])
                after_state = json.loads(log_parts[5].split("After: ")[1])
                
                rollback_query = ""
                
                if sql_operation.startswith("INSERT INTO"):
                    table = sql_operation.split(" ")[2]
                    where_clause = " AND ".join([f"{k} = {repr(v)}" for k, v in after_state.items()])
                    rollback_query = f"DELETE FROM {table} WHERE {where_clause};"
                    
                elif sql_operation.startswith("DELETE FROM"):
                    table = sql_operation.split(" ")[2]
                    columns = ", ".join(before_state.keys())
                    values = ", ".join([repr(v) for v in before_state.values()])
                    rollback_query = f"INSERT INTO {table} ({columns}) VALUES ({values});"
                    
                elif sql_operation.startswith("UPDATE"):
                    table = sql_operation.split(" ")[1]
                    set_clause = ", ".join([f"{k} = {repr(v)}" for k, v in before_state.items()])
                    where_clause = " AND ".join([f"{k} = {repr(v)}" for k, v in after_state.items()])
                    rollback_query = f"UPDATE {table} SET {set_clause} WHERE {where_clause};"
                
                #memanggil query processor untuk menjalankan rollback query (undo)
                res = None #result dari query processor
                
                #write log rollback query?
                exec_result = ExecutionResult(tx, datetime.now().isoformat(), "TRANSACTION ROLLBACK", None, res, "ROLLBACK", rollback_query)
                self.write_log(exec_result)
            
            exec_result = ExecutionResult(tx, datetime.now().isoformat(), "TRANSACTION END", None, None, "ABORT", None)
                

        