class RecoverCriteria:
    def __init__(self, timestamp: str = None, transaction_id: int = None):
        self.timestamp = timestamp
        self.transaction_id = transaction_id