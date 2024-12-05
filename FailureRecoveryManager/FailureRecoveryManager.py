import json
from datetime import datetime
from threading import Timer

from FailureRecoveryManager.ExecutionResult import ExecutionResult
from FailureRecoveryManager.RecoverCriteria import RecoverCriteria
from FailureRecoveryManager.Rows import Rows


class FailureRecoveryManager:
    """
    This class is responsible for managing the failure recovery of the mini database system.
    """

    def __init__(
        self, log_file="./FailureRecoveryManager/log.log", checkpoint_interval=60 * 5
    ):
        # Maximum number of logs in memory (array length)
        self._max_size_log = 20

        # Log file name
        self._log_file = log_file

        # Write ahead checkpoint interval (in seconds)
        self._checkpoint_interval = checkpoint_interval
        self._start_checkpoint_cron_job()

        # Write-ahead logs (in-memory)
        self._wh_logs = ["102|2024-11-21T12:02:05.678Z|IN_PROGRESS|INSERT INTO employees (id, name, salary) VALUES (2, 'Bob', 4000);|Before: None|After: [{\"id\": 2, \"name\": \"Bob\", \"salary\": 4000}]","102|2024-11-21T12:02:20.123Z|ROLLBACK|DELETE FROM employees WHERE id = 2;|Before: None|After: None","102|2024-11-21T12:02:15.789Z|ABORTED|TRANSACTION END|[{}]","103|2024-11-21T12:02:45.321Z|IN_PROGRESS|UPDATE employees SET salary = 6000 WHERE id = 1;|Before: [{\"id\": 1, \"name\": \"Alice\", \"salary\": 5000}]|After: [{\"id\": 1, \"name\": \"Alice\", \"salary\": 6000}]"]

        # Maximum number of Exe buffer (array length)
        self._max_size_buffer = 20

        # Buffer (in-memory)
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
            f"{execution_result.message}|"
            f"{execution_result.query}|"
            f"Before: {process_rows(execution_result.data_before.data) if execution_result.data_before.data else None}|"
            f"After: {process_rows(execution_result.data_after.data) if execution_result.data_after.data else None}"
        )  # execution_result.message ga kepake (?)

        # Must call save checkpoint if the write-ahead log is full
        if self.is_wh_log_full():
            self._save_checkpoint()

        # Append entry to self._wh_logs
        self._wh_logs.append(log_entry)

    def is_wh_log_full(self, spare: int = 0) -> bool:
        """
        This method checks if the write-ahead log is full
        Note: this can be changed if unit for max_size is change to memory size (KB, MB, ect)

        Args:
            spare (int): The number of extra logs that can be added to the write-ahead log before it is considered full. Default is 0.

        Returns:
            bool: True if the write-ahead log is full (with the spare value), False otherwise.
        """

        return len(self._wh_logs) >= self._max_size_log - spare

    def _save_checkpoint(self) -> None:
        """
        I.S. The write ahead log is initialized and not empty.
        F.S. The write ahead log is saved in the log.log and saved.

        For saving the wh_logs in the log.log files in every 5 minutes interval OR when the in memory write ahead log reaches its limit.

        Note that write_log method always adds the wh_log 1 element on each call and the limit is determined by the number of elements in the wh_log,
        As a result, it is impossible to have a write log that is not savable in the log.log file.
        """

        print("[FRM]: Saving checkpoint..." + str(datetime.now()))
        # Check write ahead not empty
        if len(self._wh_logs) == 0:
            print("[FRM]: No logs to save.")
            return

        # Save the wh_log to the log.log file (append to the end of the file)
        active_transactions = set()
        with open(self._log_file, "a") as file:
            for log in self._wh_logs:
                status = log.split("|")[2]
                id = log.split("|")[0]

                if status != "COMMITTED" and status != "ABORTED":
                    active_transactions.add(id)
                else:
                    active_transactions.discard(id)

                file.write(log + "\n")

            file.write(
                f"CHECKPOINT|{datetime.now().isoformat()}|{json.dumps(list(active_transactions))}\n"
            )

        # Clear the wh_log
        self._wh_logs.clear()
        print("[FRM]: Checkpoint saved.")

    def _start_checkpoint_cron_job(self) -> None:
        """
        Start the checkpoint cron job that runs every _checkpoint_interval seconds.
        Runs on a separate thread and calls _save_checkpoint method.
        """

        self.timer = Timer(self._checkpoint_interval, self._run_checkpoint_cron_job)
        self.timer.start()

    def _run_checkpoint_cron_job(self) -> None:
        """
        This method is called by the timer thread to save the checkpoint.
        """

        try:
            # Save checkpoint
            self._save_checkpoint()

            # Restart the timer
            if self.timer:
                self.timer.cancel()  # Cancel any existing timer
            self.timer = Timer(self._checkpoint_interval, self._run_checkpoint_cron_job)
            self.timer.start()

        except Exception as e:
            print(f"Error in checkpoint cron job: {e}")
    
    def _stop_checkpoint_cron_job(self):
        """
        Stop the checkpoint cron job.
        """
        if self.timer:
            self.timer.cancel()
            self.timer = None
    


    def is_buffer_full(self, spare: int = 0) -> bool:
        """
        This method checks if the buffer is full
        Note: this can be changed if unit for max_size is change to memory size (KB, MB, ect)

        Args:
            spare (int): The number of extra logs that can be added to the buffer before it is considered full. Default is 0.

        Returns:
            bool: True if the buffer is full (with the spare value), False otherwise.
        """

        return len(self._buffer) >= self._max_size_buffer - spare

    def _manage_buffer():
        """
        Handles the buffer if it reaches its limit
        """

    def _read_lines_from_end(self, file_path, chunk_size=1024):
        
        """
        This method is private method to help FailureRecoverManager to read a file from the end of the file. This method is important to FailureRecoverManager when read log file
        
        Args:
            file_path (string): path where file is located
            chunk_size (int, optional): Defaults to 1024.

        Yields:
            string: one line text from file
        """
         

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
        """
        This method is used to perform recovery on the database. The recovery process will apply to all transactions that meet the specified criteria. A transaction is considered to meet the criteria if its transaction_id or timestamp is greater than or equal to the given criteria.
        
        For transactions that have already been completed (COMMITTED or ABORTED), the recovery will involve re-executing the queries. For transactions that are still pending, the recovery will involve performing a rollback, and the final status will be marked as aborted.

        Args:
            criteria (RecoverCriteria, optional): criteria to choose what transactio need to be recover. Criteria can be transaction_id or timestamp. Defaults to None.
        """
        recovered_transactions = []
        active_transactions = set()
        if criteria and criteria.timestamp:
            checkpoint_found = False
            # baca whl
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

            # baca log file
            if not checkpoint_found:
                for log_line in self._read_lines_from_end(self._log_file):
                    log_parts = log_line.split("|")

                    if log_parts[0] == "CHECKPOINT":
                        active_transactions = set(json.loads(log_parts[2]))
                        break
                    elif log_parts[0].isdigit():
                        timestamp = log_parts[1]
                        date_timestamp = datetime.fromisoformat(timestamp)
                        date_criteria_timestamp = datetime.fromisoformat(
                            criteria.timestamp
                        )

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
                    transaction_id = int(log_parts[0])

                    if transaction_id < criteria.transaction_id:
                        break

                    recovered_transactions.insert(0, log_line)

            # baca log file
            if not checkpoint_found:
                for log_line in self._read_lines_from_end(self._log_file):
                    log_parts = log_line.split("|")

                    if log_parts[0] == "CHECKPOINT":
                        active_transactions = set(json.loads(log_parts[2]))
                        break

                    elif log_parts[0].isdigit():
                        transaction_id = int(log_parts[0])

                        if transaction_id < criteria.transaction_id:
                            break

                        recovered_transactions.insert(0, log_line)

        # redo
        print("recover tx: ", recovered_transactions)
        for recovered_transaction in recovered_transactions:
            log_parts = recovered_transaction.split("|")
            transaction_id = int(log_parts[0])
            status = log_parts[2]

            if status == "COMMITTED":
                active_transactions.discard(transaction_id)
            elif status == "ABORTED":
                active_transactions.discard(transaction_id)
            elif status == "START":
                active_transactions.add(transaction_id)
            elif status == "IN_PROGRESS" or status == "ROLLBACK":
                # memanggil query processor untuk menjalankan ulang query (redo)
                print("send query: ", transaction_id, " ", log_parts[3])

        active_transactions = sorted(active_transactions)
        print("active tx: ", active_transactions)

        # undo
        log_active_transaction = list(filter(lambda x: int(x.split("|")[0]) in active_transactions, recovered_transactions))
        log_active_transaction = sorted(log_active_transaction, key=lambda x: datetime.fromisoformat(x.split("|")[1]))
        for curr in log_active_transaction[::-1]:
            transaction_id = int(curr.split("|")[0])
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
                    where_clause = " AND ".join(
                        [f"{k} = {repr(v)}" for k, v in after_state.items()]
                    )
                    rollback_query = f"DELETE FROM {table} WHERE {where_clause};"
                    rollback_queries.append((transaction_id, rollback_query))
            elif sql_operation.startswith("DELETE FROM"):
                table = sql_operation.split(" ")[2]
                for before_state in before_states:
                    columns = ", ".join(before_state.keys())
                    values = ", ".join([repr(v) for v in before_state.values()])
                    rollback_query = (
                        f"INSERT INTO {table} ({columns}) VALUES ({values});"
                    )
                    rollback_queries.append((transaction_id, rollback_query))
            elif sql_operation.startswith("UPDATE"):
                table = sql_operation.split(" ")[1]
                for before_state, after_state in zip(before_states, after_states):
                    set_clause = ", ".join(
                        [f"{k} = {repr(v)}" for k, v in before_state.items()]
                    )
                    where_clause = " AND ".join(
                        [f"{k} = {repr(v)}" for k, v in after_state.items()]
                    )
                    rollback_query = (
                        f"UPDATE {table} SET {set_clause} WHERE {where_clause};"
                    )
                    rollback_queries.append((transaction_id, rollback_query))
        print("rollback query: ", rollback_queries)
        for tx_id, rb_query in rollback_queries:
            print("query rollback: ", rb_query)
            
            #panggil query procesor to process rollback query
            res = None
            
            exec_result = ExecutionResult(
                tx_id,
                datetime.now().isoformat(),
                "TRANSACTION ROLLBACK",
                Rows(None),
                Rows(res),
                "ROLLBACK",
                rb_query,
            )
            print("write log: ", exec_result.__dict__)
            self.write_log(exec_result)
        
        #close undo process give abort status
        for tx_id in active_transactions:
            exec_result = ExecutionResult(
                tx_id,
                datetime.now().isoformat(),
                "TRANSACTION END",
                Rows(None),
                Rows(None),
                "ABORTED",
                None,
            )
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

        # redo
        for recovered_transaction in recovered_transactions:
            log_parts = recovered_transaction.split("|")
            transaction_id = int(log_parts[0])
            status = log_parts[2]

            if status == "COMMITTED":
                active_transactions.discard(transaction_id)
            elif status == "ABORTED":
                active_transactions.discard(transaction_id)
            elif status == "START":
                active_transactions.add(transaction_id)
            elif status == "IN_PROGRESS" or status == "ROLLBACK":
                # memanggil query processor untuk menjalankan ulang query (redo)
                pass

        # undo
        log_active_transaction = list(filter(lambda x: int(x.split("|")[0]) in active_transactions, recovered_transactions))
        log_active_transaction = sorted(log_active_transaction, key=lambda x: datetime.fromisoformat(x.split("|")[1]))
        for curr in log_active_transaction[::-1]:
            transaction_id = int(curr.split("|")[0])
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
                    where_clause = " AND ".join(
                        [f"{k} = {repr(v)}" for k, v in after_state.items()]
                    )
                    rollback_query = f"DELETE FROM {table} WHERE {where_clause};"
                    rollback_queries.append((transaction_id, rollback_query))
            elif sql_operation.startswith("DELETE FROM"):
                table = sql_operation.split(" ")[2]
                for before_state in before_states:
                    columns = ", ".join(before_state.keys())
                    values = ", ".join([repr(v) for v in before_state.values()])
                    rollback_query = (
                        f"INSERT INTO {table} ({columns}) VALUES ({values});"
                    )
                    rollback_queries.append((transaction_id, rollback_query))
            elif sql_operation.startswith("UPDATE"):
                table = sql_operation.split(" ")[1]
                for before_state, after_state in zip(before_states, after_states):
                    set_clause = ", ".join(
                        [f"{k} = {repr(v)}" for k, v in before_state.items()]
                    )
                    where_clause = " AND ".join(
                        [f"{k} = {repr(v)}" for k, v in after_state.items()]
                    )
                    rollback_query = (
                        f"UPDATE {table} SET {set_clause} WHERE {where_clause};"
                    )
                    rollback_queries.append((transaction_id, rollback_query))
        print("rollback query: ", rollback_queries)
        for tx_id, rb_query in rollback_queries:
            print("query rollback: ", rb_query)
            
            #panggil query procesor to process rollback query
            res = None
            
            exec_result = ExecutionResult(
                tx_id,
                datetime.now().isoformat(),
                "TRANSACTION ROLLBACK",
                Rows(None),
                Rows(res),
                "ROLLBACK",
                rb_query,
            )
            print("write log: ", exec_result.__dict__)
            self.write_log(exec_result)
        
        #close undo process give abort status
        for tx_id in active_transactions:
            exec_result = ExecutionResult(
                tx_id,
                datetime.now().isoformat(),
                "TRANSACTION END",
                Rows(None),
                Rows(None),
                "ABORTED",
                None,
            )
            print("write log: ", exec_result.__dict__)

            self.write_log(exec_result)
