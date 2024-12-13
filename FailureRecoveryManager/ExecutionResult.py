from typing import Union, Optional, List
from FailureRecoveryManager.Rows import Rows

class ExecutionResult:
 
    def __init__(self, 
                 transaction_id: int, 
                 data_before: Optional[Union[Rows, int]], 
                 data_after: Optional[Union[Rows, int]], 
                 status: str, 
                 table: str):
        
        self.transaction_id = transaction_id
        self.data_before = data_before
        self.data_after = data_after
        self.status = status
        self.table = table