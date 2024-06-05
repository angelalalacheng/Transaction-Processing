import socket
import pickle
from Partition import PartitionStrategy
from Transaction import Transaction
from Hop import Hop

class Client:
    def __init__(self, servers, id, location):
        self.servers = servers
        self.id = id
        self.location = location

    def send_hop(self, server, hop, return_value=None):
        server_host, server_port = self.servers[server]
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((server_host, server_port))
            hop_data = {'hop': hop, 'return_value': return_value}
            s.sendall(pickle.dumps(hop_data))
            response = s.recv(1024)
            return pickle.loads(response)

    def send_transaction(self, transaction):
        return_value = None
        # Execute the first hop
        first_hop = transaction.hops[0]
        response = self.send_hop(first_hop.node, first_hop)
        
        if response.get('status') == 'Failed':
            print(f"## Transaction {transaction.transaction_id} aborted at first hop {first_hop.node}")
            return
        return_value = response.get('return_value', None)
        print(f"First hop {first_hop.node} completed successfully")

        # Execute subsequent hops with retry logic
        for hop in transaction.hops[1:]:
            result = self.send_hop(hop.node, hop, return_value)
            return_value = result.get("return_value", None)    

        print(f"## Transaction {transaction.transaction_id} completed successfully")



    def add_user(self, t_id, params):
        transaction = Transaction(transaction_id=t_id, type='add_user', hops=[
            Hop(hop_id=1, node=self.location, action='add_user', parameters=params)
        ])
        self.send_transaction(transaction)
    

    def add_book(self, t_id, params):
        transaction = Transaction(transaction_id=t_id, type='add_book', hops=[
            Hop(hop_id=1, node=self.location, action='add_book', parameters=params)
        ])
        self.send_transaction(transaction)


    def delete_book(self, t_id, params):
        transaction = Transaction(transaction_id=t_id, type='delete_book', hops=[
            Hop(hop_id=1, node=self.book_location(params.get("book_id")), action='delete_book', parameters=params)
        ])
        self.send_transaction(transaction)


    def query_user(self, t_id, params):
        transaction = Transaction(transaction_id=t_id, type='query_user', hops=[
            Hop(hop_id=1, node=self.location, action='query_user', parameters=params)
        ])
        self.send_transaction(transaction)

    
    def borrow_book(self, t_id, params):
        book_location = self.book_location(params['book_id'])
        other_locations = self.other_locations(book_location)

        transaction = Transaction(transaction_id=t_id, type='borrow_book', hops=[
            Hop(hop_id=1, node=book_location, action='borrow_book', parameters=params),
            Hop(hop_id=2, node=other_locations[0], action='add_loan', parameters=params),
            Hop(hop_id=3, node=other_locations[1], action='add_loan', parameters=params),
        ])
        self.send_transaction(transaction)
    

    def return_book(self, t_id, params):
        book_location = self.book_location(params['book_id'])
        other_locations = self.other_locations(book_location)

        transaction = Transaction(transaction_id=t_id, type='return_book', hops=[
            Hop(hop_id=1, node=book_location, action='return_book', parameters=params),
            Hop(hop_id=2, node=other_locations[0], action='update_loan', parameters=params),
            Hop(hop_id=3, node=other_locations[1], action='update_loan', parameters=params),
        ])
        self.send_transaction(transaction)


    def track_loans(self, t_id, params):
        transaction = Transaction(transaction_id=t_id, type='track_loans', hops=[
            Hop(hop_id=1, node=self.location, action='track_loans', parameters = params),
        ])
        self.send_transaction(transaction)


    def book_location(self, book_id):
        start_digit = round(book_id / 1000)
        if start_digit == 1:
            return "Library A"
        elif start_digit == 2:
            return "Library B"
        else:
            return "Library C"
        
    
    def other_locations(self, node):
        nodes = ['Library A', 'Library B', 'Library C']
        nodes.remove(node)
        return nodes


if __name__ == "__main__":
    servers = {
        'Library A': ('localhost', 9000),
        'Library B': ('localhost', 9001),
        'Library C': ('localhost', 9002)
    }

    librarian1 = Client(servers, 1001, 'Library A')
    librarian2 = Client(servers, 2001, 'Library B')
    librarian3 = Client(servers, 3001, 'Library C')
    member1 = Client(servers, 1002, 'Library A')

    # T1
    member1.borrow_book(1, {'book_id': 2002, 
                            'user_id': member1.id, 
                            'borrow_date': '2023-02-01', 
                            'due_date': '2023-03-01'})
    
    member1.borrow_book(8, {'book_id': 4002, 
                            'user_id': member1.id, 
                            'borrow_date': '2023-02-01', 
                            'due_date': '2023-03-01'})
    # T2
    librarian1.add_user(2, {'user_id': 1004, 
                            'name': 'User 4', 
                            'email': 'user4@example.com', 
                            'membership': librarian1.location})
    # T3
    librarian2.add_book(3, {'book_id': 2004, 
                            'title': 'Book 4', 
                            'author': 'Author 4', 
                            'publication_date': '2023-01-01', 
                            'category': 'Fiction', 
                            'status': 'Available'})
    # T4
    librarian3.delete_book(4, {'book_id': 3001})
    # T5
    librarian1.query_user(5, {'user_id': 1004})    
    # T6
    librarian2.track_loans(6, {})
    # T7
    member1.return_book(7, {'book_id': 2002, 
                            'return_date': '2023-02-10'})


