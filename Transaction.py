class Transaction:
    def __init__(self, transaction_id, type, hops):
        self.transaction_id = transaction_id
        self.type = type
        self.hops = hops
    
    