import json
from datetime import datetime
from threading import Lock, Timer
from typing import Union

from FailureRecoveryManager.ExecutionResult import ExecutionResult
from FailureRecoveryManager.LRUCache import LRUCache
from FailureRecoveryManager.RecoverCriteria import RecoverCriteria
from FailureRecoveryManager.Rows import Rows
from StorageManager.classes import Condition, DataDeletion, DataWrite, StorageManager

from . import Buffer


class FailureRecoveryManager:
    """
    This class is responsible for managing the failure recovery of the mini database system.
    """

    def __init__(
        self,
        buffer: Buffer,
        log_file="./FailureRecoveryManager/log.log",
        checkpoint_interval=5,
        storage_manager=StorageManager()
    ):
        """
        Dependency injection
        """
        self.buffer = buffer

        """
        WRITE AHEAD LOG
        """
        # Maximum number of logs in memory (array length)
        self._max_size_log = 20
        # Log file name
        self._log_file = log_file
        # Write-ahead logs (in-memory)
        # self._wa_logs = [
        #     '102|WRITE|employees|None|[{"id": 2, "name": "Bob", "salary": 4000}]',
        #     '102|WRITE|employees|[{"id": 2, "name": "Bob", "salary": 4000}]|None',
        #     "102|ABORT",
        #     '103|WRITE|employees|[{"id": 1, "name": "Alice", "salary": 5000}]|[{"id": 1, "name": "Alice", "salary": 6000}]',
        # ]
        self._wa_logs = []
        # Write-ahead log mutex
        self._wa_log_lock = Lock()

        """
        STORAGE MANAGER CLASS
        """
        self.storage = storage_manager

        """
        SAVE CHECKPOINT CRON JOB
        """
        self._checkpoint_interval = checkpoint_interval
        self.timer = None
        self._start_checkpoint_cron_job()

    def __del__(self):
        # print("Destroy Failure Recovery Manager...")
        self._stop_checkpoint_cron_job()

    def write_log(self, execution_result: ExecutionResult) -> None:
        # This method accepts execution result object as input and appends
        # an entry in a write-ahead log based on execution info object.

        # Create log entry
        def process_rows(rows):
            return json.dumps(rows)

        log_entry = None

        if execution_result.status == "WRITE":
            log_entry = (
                f"{execution_result.transaction_id}|"
                f"{execution_result.status}|"
                f"{execution_result.table}|"
                f"{process_rows(execution_result.data_before.data) if execution_result.data_before.data else None}|"
                f"{process_rows(execution_result.data_after.data) if execution_result.data_after.data else None}"
            )
        else:
            log_entry = (
                f"{execution_result.transaction_id}|" f"{execution_result.status}"
            )

        # Must write wal to stable storage if the write-ahead log is full
        self._wa_log_lock.acquire()
        if self.is_wa_log_full():
            self._wa_log_lock.release()
            # self._save_checkpoint()
            self._flush_wal()
        self._wa_log_lock.release()

        # Append entry to self._wa_logs
        self._wa_log_lock.acquire()
        self._wa_logs.append(log_entry)
        self._wa_log_lock.release()

        # Must write wal to stable storage if transaction commited or aborted to ensure consistency
        if execution_result.status == "COMMIT" or execution_result.status == "ABORT":
            # self._save_checkpoint()
            self._flush_wal()

    def is_wa_log_full(self, spare: int = 0) -> bool:
        """
        This method checks if the write-ahead log is full
        Note: this can be changed if unit for max_size is change to memory size (KB, MB, ect)

        Args:
            spare (int): The number of extra logs that can be added to the write-ahead log before it is considered full. Default is 0.

        Returns:
            bool: True if the write-ahead log is full (with the spare value), False otherwise.
        """

        return len(self._wa_logs) >= self._max_size_log - spare

    def _save_checkpoint(self) -> None:
        """
        I.S. The write ahead log and buffer exists.
        F.S. 1. The write ahead log is saved in the log.log and and cleared
             2. The buffer all is written to the disk and cleared.

        For saving the wh_logs in the log.log files in every 5 minutes interval OR when the in memory write ahead log reaches its limit.
        Buffer is also written to the disk and also cleared.

        Note that write_log method always adds the wa_log 1 element on each call and the limit is determined by the number of elements in the wa_log,
        As a result, it is impossible to have a write log that is not savable in the log.log file.
        """

        # print(f"[FRM | {str(datetime.now())}]: Saving checkpoint...")

        # MANAGE BUFFER
        # TODO: Write all the buffer to the disk . Need to inject storage manager

        # Clear the buffer
        self.buffer.clear_buffer()
        # print(f"[FRM | {str(datetime.now())}]: Buffer cleared.")

        self._wa_log_lock.acquire()
        # MANAGE WA LOG
        # Check write ahead not empty
        if len(self._wa_logs) == 0:
            # print(f"[FRM | {str(datetime.now())}]: No logs to save.")
            return

        # Save the wh_log to the log.log file (append to the end of the file)\
        try:
            # After transaction is commited or aborted, it cannot do more operation.
            # So it is guarenteed that the remaining transaction id in the set is active.
            active_transactions = set()
            with open(self._log_file, "a") as file:
                for log in self._wa_logs:
                    status = log.split("|")[1]
                    id = log.split("|")[0]
                    if status != "COMMIT" and status != "ABORT":
                        active_transactions.add(id)
                    else:
                        active_transactions.discard(id)
                    file.write(log + "\n")
                file.write(f"CHECKPOINT|{json.dumps(list(active_transactions))}\n")
            # Clear the wh_log
            self._wa_logs.clear()
            # print(f"[FRM | {str(datetime.now())}]: write ahead log saved.")
        except Exception as e:
            # print(f"[FRM | {str(datetime.now())}]: Error saving checkpoint: {e}")
            pass
        finally:
            self._wa_log_lock.release()

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
            # print(f"Error in checkpoint cron job: {e}")
            pass

    def _stop_checkpoint_cron_job(self):
        """
        Stop the checkpoint cron job.
        """
        if self.timer:
            self.timer.cancel()
            self.timer = None

    def _flush_wal(self):
        with open(self._log_file, "a") as file:
            self._wa_log_lock.acquire()

            for line in self._wa_logs:
                file.write(line + "\n")

            self._wa_log_lock.release()

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
        The recover function is used to perform a rollback on a transaction based on the criteria specified in the RecoverCriteria. To recover the before and after data recorded in the log will be sent to the Storage Manager for processing.

        Args:
            criteria (RecoverCriteria, optional): criteria to choose what transactio need to be recover. Criteria is transaction_id. Defaults to None.
        """

        # Hanya menggunakan transaction id saja sekarang

        recovered_transactions = []
        is_start_found = False
        # baca wa_logs dulu
        self._wa_log_lock.acquire()
        for log_line in self._wa_logs:
            log_parts = log_line.strip().split("|")
            if log_parts[0].isdigit():
                transaction_id = int(log_parts[0])
                if transaction_id == criteria.transaction_id:
                    if log_parts[1] == "START":
                        is_start_found = True
                        break
                    recovered_transactions.append(log_line)
        # baca log file
        if not is_start_found:
            for log_line in self._read_lines_from_end(self._log_file):
                log_parts = log_line.strip().split("|")
                if log_parts[0].isdigit():
                    transaction_id = int(log_parts[0])
                    if transaction_id == criteria.transaction_id:
                        if log_parts[1] == "START":
                            is_start_found = True
                            break
                        recovered_transactions.append(log_line)
        self._wa_log_lock.release()

        # undo (rollback)
        table = ""
        for curr in recovered_transactions:
            log_parts = curr.strip().split("|")
            transaction_id = int(log_parts[0])
            before_states = None
            after_states = None
            table = log_parts[2]
            try:
                before_states_raw = log_parts[3].strip()
                after_states_raw = log_parts[4].strip()
                if before_states_raw == "None":
                    before_states = None
                else:
                    before_states = json.loads(before_states_raw)
                if after_states_raw == "None":
                    after_states = None
                else:
                    after_states = json.loads(after_states_raw)
            except json.JSONDecodeError as e:
                # print(f"Error decoding JSON: {e}")
                exit()
            # send before and after data to storage manager to process

            # print("table:", table)
            # print("send before to storage manager: ", before_states)
            # print("send after to storage manager: ", after_states)

            # insert case
            if before_states == None and after_states != None:
                for state in after_states:
                    data_write = DataWrite(table, state.keys(), state.values(), "")
                    affected = self.storage.write_block(data_write)
                    if affected == 1:
                        # print(f"[FRM | {str(datetime.now())}]: success write block for rollback query.")
                        pass
                    else:
                        # print(f"[FRM | {str(datetime.now())}]: failed write block for rollback query.")
                        exit()

            # delete case
            elif before_states != None and after_states == None:
                for state in before_states:
                    conditions = []
                    for column, value in state.items():
                        new_condition = Condition(column, "=", value)
                        conditions.append(new_condition)
                    data_deletion = DataDeletion(table, conditions, "")
                    affected = self.storage.delete_block(data_deletion)
                    if affected == 1:
                        # print(f"[FRM | {str(datetime.now())}]: success delete block for rollback query.")
                        pass
                    else:
                        # print(f"[FRM | {str(datetime.now())}]: failed delete block for rollback query.")
                        exit()

            # udpdate case
            elif before_states != None and after_states != None:
                for i in range(len(before_states)):
                    conditions = []
                    for column, value in before_states[i].items():
                        new_condition = Condition(column, "=", value)
                        conditions.append(new_condition)
                    data_write = DataWrite(
                        table,
                        after_states[i].keys(),
                        after_states[i].values(),
                        "",
                        conditions=conditions,
                    )
                    affected = self.storage.write_block(data_write)
                    if affected == 1:
                        # print(f"[FRM | {str(datetime.now())}]: success delete block for rollback query.")
                        pass
                    else:
                        # print(f"[FRM | {str(datetime.now())}]: failed delete block for rollback query.")
                        exit()

            # Write recovery log
            exec_result = ExecutionResult(
                transaction_id,
                Rows(after_states),
                Rows(before_states),
                "WRITE",
                table,
            )
            # print("write log: ", exec_result.__dict__)
            self.write_log(exec_result)
        # Close rollback process
        abort_result = ExecutionResult(
            criteria.transaction_id,
            Rows(None),
            Rows(None),
            "ABORT",
            "",
        )
        # print("write log: ", exec_result.__dict__)
        self.write_log(abort_result)

    def recover_system_crash(self):
        """
        This function is used to perform recovery in the event of a system crash.
        The recovery process will start from the last recorded CHECKPOINT in the log file.
        For transactions that have already been completed (COMMIT or ABORT), the transaction will be redone,
        while for transactions that are incomplete,a rollback (UNDO) will be performed.
        """

        recovered_transactions = []
        active_transactions = set()
        for log_line in self._read_lines_from_end(self._log_file):
            log_parts = log_line.strip().split("|")
            if log_parts[0] == "CHECKPOINT":
                active_transactions = set(json.loads(log_parts[1]))
                break
            elif log_parts[0].isdigit():
                recovered_transactions.insert(0, log_line)
        # redo
        for recovered_transaction in recovered_transactions:
            log_parts = recovered_transaction.strip().split("|")
            transaction_id = int(log_parts[0])
            status = log_parts[1]
            if status == "COMMIT":
                active_transactions.discard(transaction_id)
            elif status == "ABORT":
                active_transactions.discard(transaction_id)
            elif status == "START":
                active_transactions.add(transaction_id)
            elif status == "WRITE":
                before_states = None
                after_states = None
                table = log_parts[2]
                try:
                    before_states_raw = log_parts[3].strip()
                    after_states_raw = log_parts[4].strip()
                    if before_states_raw == "None":
                        before_states = None
                    else:
                        before_states = json.loads(before_states_raw)
                    if after_states_raw == "None":
                        after_states = None
                    else:
                        after_states = json.loads(after_states_raw)
                except json.JSONDecodeError as e:
                    # print(f"Error decoding JSON: {e}")
                    exit()
                # send before after data to storage manager to redo process
                # print("table:", table)
                # print("send before data to storage manager: ", before_states)
                # print("send after data to storage manager: ", after_states)

                # insert case
                if before_states == None and after_states != None:
                    for state in after_states:
                        data_write = DataWrite(table, state.keys(), state.values(), "")
                        affected = self.storage.write_block(data_write)
                        # if (affected == 1):
                        # print(f"[FRM | {str(datetime.now())}]: success write block for rollback query.")
                        # else:
                        # print(f"[FRM | {str(datetime.now())}]: failed write block for rollback query.")
                        # exit()

                # delete case
                elif before_states != None and after_states == None:
                    for state in before_states:
                        conditions = []
                        for column, value in state.items():
                            new_condition = Condition(column, "=", value)
                            conditions.append(new_condition)
                        data_deletion = DataDeletion(table, conditions, "")
                        affected = self.storage.delete_block(data_deletion)
                        if affected == 1:
                            # print(f"[FRM | {str(datetime.now())}]: success delete block for rollback query.")
                            pass
                        else:
                            # print(f"[FRM | {str(datetime.now())}]: failed delete block for rollback query.")
                            exit()

                # udpdate case
                elif before_states != None and after_states != None:
                    for i in range(len(before_states)):
                        conditions = []
                        for column, value in before_states[i].items():
                            new_condition = Condition(column, "=", value)
                            conditions.append(new_condition)
                        data_write = DataWrite(
                            table,
                            after_states[i].keys(),
                            after_states[i].values(),
                            "",
                            conditions=conditions,
                        )
                        affected = self.storage.write_block(data_write)
                        if affected == 1:
                            # print(f"[FRM | {str(datetime.now())}]: success delete block for rollback query.")
                            pass
                        else:
                            # print(f"[FRM | {str(datetime.now())}]: failed delete block for rollback query.")
                            exit()

        # undo (rollback)
        for log_line in self._read_lines_from_end(self._log_file):
            log_parts = log_line.strip().split("|")
            if log_parts[0] == "CHECKPOINT":
                continue
            elif log_parts[0].isdigit():
                transaction_id = int(log_parts[0])
                status = log_parts[1]
                if transaction_id not in active_transactions:
                    continue
                if status == "START":
                    active_transactions.remove(transaction_id)
                    # Close rollback process
                    abort_result = ExecutionResult(
                        transaction_id,
                        Rows(None),
                        Rows(None),
                        "ABORT",
                        "",
                    )
                    # print("write log: ", abort_result.__dict__)
                    self.write_log(abort_result)
                    continue
                # rollback processs
                before_states = None
                after_states = None
                table = log_parts[2]
                try:
                    before_states_raw = log_parts[3].strip()
                    after_states_raw = log_parts[4].strip()
                    if before_states_raw == "None":
                        before_states = None
                    else:
                        before_states = json.loads(before_states_raw)
                    if after_states_raw == "None":
                        after_states = None
                    else:
                        after_states = json.loads(after_states_raw)
                except json.JSONDecodeError as e:
                    # print(f"Error decoding JSON: {e}")
                    exit()
                # send before and after data to storage manager to process
                # print("table:", table)
                # print("send before to storage manager: ", before_states)
                # print("send after to storage manager: ", after_states)
                # insert case
                if before_states == None and after_states != None:
                    for state in after_states:
                        data_write = DataWrite(table, state.keys(), state.values(), "")
                        affected = self.storage.write_block(data_write)
                        if affected == 1:
                            # print(f"[FRM | {str(datetime.now())}]: success write block for rollback query.")
                            pass
                        else:
                            # print(f"[FRM | {str(datetime.now())}]: failed write block for rollback query.")
                            exit()

                # delete case
                elif before_states != None and after_states == None:
                    for state in before_states:
                        conditions = []
                        for column, value in state.items():
                            new_condition = Condition(column, "=", value)
                            conditions.append(new_condition)
                        data_deletion = DataDeletion(table, conditions, "")
                        affected = self.storage.delete_block(data_deletion)
                        if affected == 1:
                            # print(f"[FRM | {str(datetime.now())}]: success delete block for rollback query.
                            pass
                        else:
                            # print(f"[FRM | {str(datetime.now())}]: failed delete block for rollback query.")
                            exit()

                # udpdate case
                elif before_states != None and after_states != None:
                    for i in range(len(before_states)):
                        conditions = []
                        for column, value in before_states[i].items():
                            new_condition = Condition(column, "=", value)
                            conditions.append(new_condition)
                        data_write = DataWrite(
                            table,
                            after_states[i].keys(),
                            after_states[i].values(),
                            "",
                            conditions=conditions,
                        )
                        affected = self.storage.write_block(data_write)
                        if affected == 1:
                            # print(f"[FRM | {str(datetime.now())}]: success delete block for rollback query.")
                            pass
                        else:
                            # print(f"[FRM | {str(datetime.now())}]: failed delete block for rollback query.")

                            exit()

                # Write recovery log
                exec_result = ExecutionResult(
                    transaction_id,
                    Rows(after_states),
                    Rows(before_states),
                    "WRITE",
                    table,
                )
                # print("write log: ", exec_result.__dict__)
                self.write_log(exec_result)
            if len(active_transactions) == 0:
                break
            if len(active_transactions) == 0:
                break
