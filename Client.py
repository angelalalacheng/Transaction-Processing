import socket
import pickle
import threading
from time import sleep
from Transaction import Transaction
from Hop import Hop

class Client:
    def __init__(self, servers, id, location, max_retries=3):
        self.servers = servers
        self.id = id
        self.location = location
        self.max_retries = max_retries
        self.lock = threading.Lock()

    def send_hop(self, server, hop, return_value = None, sequence_number = None):
        server_host, server_port = self.servers[server]
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((server_host, server_port))
            hop_data = {'hop': hop, 'return_value': return_value}
            if sequence_number is not None:
                hop_data['sequence_number'] = sequence_number
            s.sendall(pickle.dumps(hop_data))
            response = s.recv(1024)
            return pickle.loads(response)
        
    def send_transaction(self, transaction):
        raise NotImplementedError("Must be implemented by subclass.")


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


class BaseClient(Client):
    def send_transaction(self, transaction):
        # with self.lock: 
            return_value = None
            # Execute the first hop
            first_hop = transaction.hops[0]
            response = self.send_hop(first_hop.node, first_hop)
            
            if response.get('status') == 'Failed':
                print(f"## Transaction {transaction.transaction_id} aborted at first hop {first_hop.node}")
                return
            return_value = response.get('return_value', None)
            print(f"First hop {first_hop.node} completed successfully for Transaction {transaction.transaction_id}")

            # Execute the rest of the hops and retry if necessary
            for hop in transaction.hops[1:]:
                retries = 0
                print(f"Executing hop {hop.hop_id} for Transaction {transaction.transaction_id}")
                while retries < self.max_retries:
                    response = self.send_hop(hop.node, hop, return_value)
                    if response.get('status') == 'Success':
                        return_value = response.get('return_value', None)
                        break
                    else:
                        retries += 1
                        print(f"Retrying hop {hop.hop_id} for transaction {transaction.transaction_id}, attempt {retries}")
                        sleep(1)
                if retries == self.max_retries:
                    print(f"Transaction {transaction.transaction_id} failed at hop {hop.node} after {self.max_retries} retries")
                    return

            print(f"## Transaction {transaction.transaction_id} completed successfully")

class OriginOrderClient(Client):
    def get_sequence_number(self):
        server_host, server_port = self.servers[self.location]
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((server_host, server_port))
            s.sendall(pickle.dumps({'get_sequence_number': True}))
            sequence_number = pickle.loads(s.recv(1024))
            return sequence_number
        
    def send_transaction(self, transaction):
        with self.lock:
            sequence_number = self.get_sequence_number()
            transaction.sequence_number = sequence_number
            print(f"## Sequence number for T{transaction.transaction_id}: {sequence_number}")

            return_value = None

            first_hop = transaction.hops[0]
            first_hop.sequence_number = sequence_number
            response = self.send_hop(first_hop.node, first_hop, sequence_number=sequence_number)

            if response.get('status') == 'Failed':
                print(f"## Transaction {transaction.transaction_id} aborted at first hop {first_hop.node}")
                return
            return_value = response.get('return_value', None)
            print(f"First hop {first_hop.node} completed successfully for Transaction {transaction.transaction_id}")

            for hop in transaction.hops[1:]:
                retries = 0
                hop.sequence_number = sequence_number
                print(f"Executing hop {hop.hop_id} for Transaction {transaction.transaction_id}")
                while retries < self.max_retries:
                    response = self.send_hop(hop.node, hop, return_value, sequence_number=sequence_number)
                    if response.get('status') == 'Success':
                        return_value = response.get('return_value', None)
                        break
                    else:
                        retries += 1
                        print(f"Retrying hop {hop.hop_id} for transaction {transaction.transaction_id}, attempt {retries}")
                        sleep(1)
                if retries == self.max_retries:
                    print(f"Transaction {transaction.transaction_id} failed at hop {hop.node} after {self.max_retries} retries")
                    return

            print(f"## Transaction {transaction.transaction_id} completed successfully")



if __name__ == "__main__":
    servers = {
        'Library A': ('localhost', 9000),
        'Library B': ('localhost', 9001),
        'Library C': ('localhost', 9002)
    }

    librarian1 = OriginOrderClient(servers, 1001, 'Library A')
    librarian2 = OriginOrderClient(servers, 2001, 'Library B')
    librarian3 = OriginOrderClient(servers, 3001, 'Library C')
    member1 = OriginOrderClient(servers, 1002, 'Library A')
    
    # # T1
    # member1.borrow_book(1, {'book_id': 2002, 
    #                         'user_id': member1.id, 
    #                         'borrow_date': '2023-02-01', 
    #                         'due_date': '2023-03-01'})
    
    # # T6
    # librarian2.track_loans(6, {}) 

    # # T2
    # librarian1.add_user(2, {'user_id': 1004, 
    #                         'name': 'User 4', 
    #                         'email': 'user4@example.com', 
    #                         'membership': librarian1.location})
    # # T3
    # librarian2.add_book(3, {'book_id': 2004, 
    #                         'title': 'Book 4', 
    #                         'author': 'Author 4', 
    #                         'publication_date': '2023-01-01', 
    #                         'category': 'Fiction', 
    #                         'status': 'Available'})
    # # T4
    # librarian3.delete_book(4, {'book_id': 3001})
    # # T5
    # librarian1.query_user(5, {'user_id': 1004})   


    # # T7
    # member1.return_book(7, {'book_id': 2002, 
    #                         'return_date': '2023-02-10'})

    threads = []

    t1 = threading.Thread(target=member1.borrow_book, args=(1, {'book_id': 2002, 
                                                                'user_id': member1.id, 
                                                                'borrow_date': '2023-02-01', 
                                                                'due_date': '2023-03-01'}))
    threads.append(t1)

    t2 = threading.Thread(target=librarian1.add_user, args=(2, {'user_id': 1004, 
                                                                'name': 'User 4', 
                                                                'email': 'user4@example.com', 
                                                                'membership': librarian1.location}))
    threads.append(t2)

    t3 = threading.Thread(target=librarian2.add_book, args=(3, {'book_id': 2004, 
                                                                'title': 'Book 4', 
                                                                'author': 'Author 4', 
                                                                'publication_date': '2023-01-01', 
                                                                'category': 'Fiction', 
                                                                'status': 'Available'}))
    threads.append(t3)

    t4 = threading.Thread(target=librarian3.delete_book, args=(4, {'book_id': 3001}))
    threads.append(t4)

    t5 = threading.Thread(target=librarian1.query_user, args=(5, {'user_id': 1004}))
    threads.append(t5)

    t6 = threading.Thread(target=librarian2.track_loans, args=(6, {}))
    threads.append(t6)

    t7 = threading.Thread(target=member1.return_book, args=(7, {'book_id': 2002, 
                                                                'return_date': '2023-02-10'}))
    threads.append(t7)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()