class Hop:
    def __init__(self, hop_id, node, action, parameters, sequence_number = None):
        self.hop_id = hop_id
        self.node = node
        self.action = action
        self.parameters = parameters
        self.sequence_number = sequence_number