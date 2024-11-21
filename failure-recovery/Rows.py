from typing import List, Any


class Rows:
    def __init__(self, data: List[Any]):
        self.data = data 
        self.rows_count = len(data) 