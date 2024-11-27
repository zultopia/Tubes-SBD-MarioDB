from typing import List, Any


class Rows:
    def __init__(self, data: List[Any]):
        if data:
            self.data = data 
            self.rows_count = len(data) 
        else:
            self.data = None 
            self.rows_count = None 