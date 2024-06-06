class Transaction:
    def __init__(self, transaction_id, type, hops, sequence_number = None):
        self.transaction_id = transaction_id
        self.type = type
        self.hops = hops
        self.sequence_number = sequence_number

    
    