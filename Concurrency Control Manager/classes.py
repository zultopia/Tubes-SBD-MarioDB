# import class Row
from datetime import datetime

class Row:
    def __init__(self):
        pass

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
        return int(datetime.now().strftime("%Y%m%d%H%M%S%f"))
    
    def begin_transaction(self) -> int:
        # will return transaction_id: int
        
        self.__generate_id
        pass
    
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
    