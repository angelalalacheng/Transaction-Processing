class Transaction:
    def __init__(self, transaction_id, type, payload, hops):
        self.transaction_id = transaction_id
        self.type = type
        self.payload = payload
        self.hops = hops