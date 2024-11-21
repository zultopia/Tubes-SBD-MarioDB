import json
from RecoverCriteria import RecoverCriteria

class FailureRecoveryManager:
    def __init__(self, log_file="log.log"):
        self.log_file = log_file
        self.wh_logs = []
                
    def write_log(self, execution_result):
        pass
    
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
        for log in self._read_lines_from_end(self.log_file):
            parts = log.strip().split('|')

        