import socket
import pickle
import threading
from Transaction import Transaction
from Hop import Hop

class Client:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def send_transaction(self, transaction):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.host, self.port))
            request = pickle.dumps(transaction)
            s.sendall(request)
            response = s.recv(1024)
        return pickle.loads(response)

def execute_use_case(client, transaction):
    response = client.send_transaction(transaction)
    if  response:
        print(response)
    else:
        print("## Error processing transaction")

if __name__ == "__main__":
    # 定义客户端连接的主机和端口
    client = Client('localhost', 9001)

    # 定义所有用例的交易
    transactions = []

    # case1: add new book
    transactions.append(
        Transaction(transaction_id=1, type='T1', payload={}, hops=[
            Hop(hop_id=1, node=9001, action='add_book', parameters={
                'book_id': 1, 'title': 'Book 1', 'author': 'Author 1', 'publication_date': '2023-01-01', 'category': 'Fiction', 'status': 'Available'
            }),
            Hop(hop_id=1, node=9002, action='add_book', parameters={
                'book_id': 2, 'title': 'Book 2', 'author': 'Author 2', 'publication_date': '2013-01-01', 'category': 'History', 'status': 'Available'
            }),
            Hop(hop_id=1, node=9003, action='add_book', parameters={
                'book_id': 3, 'title': 'Book 3', 'author': 'Author 3', 'publication_date': '2001-01-01', 'category': 'Romance', 'status': 'Available'
            }),
        ])
    )

    # case2: add new member
    transactions.append(
        Transaction(transaction_id=2, type='T2', payload={}, hops=[
            Hop(hop_id=1, node=9001, action='add_user', parameters={
                'user_id': 1, 'name': 'User 1', 'email': 'user1@example.com', 'membership': 'Library A'
            }),
             Hop(hop_id=2, node=9002, action='add_user', parameters={
                'user_id': 2, 'name': 'User 2', 'email': 'user2@example.com', 'membership': 'Library B'
            })
        ])
    )

    # case3: borrow book
    transactions.append(
        Transaction(transaction_id=3, type='T3', payload={}, hops=[
            Hop(hop_id=1, node=9001, action='query_user', parameters={
                'user_id': 1
            }),
            Hop(hop_id=2, node=9002, action='query_book', parameters={
                'book_id': 2
            }),
            Hop(hop_id=3, node=9002, action='borrow_book', parameters={
                'book_id': 2, 'user_id': 1, 'borrow_date': '2023-02-01', 'due_date': '2023-03-01'
            }),
        ])
    )

    # case4: return book
    transactions.append(
        Transaction(transaction_id=4, type='T4', payload={}, hops=[
            Hop(hop_id=1, node=9001, action='query_user', parameters={
                'user_id': 1
            }),
            Hop(hop_id=2, node=9002, action='check_loan', parameters={
                'loan_id': 1, 'user_id': 1
            }),
            Hop(hop_id=3, node=9001, action='return_book', parameters={
                'loan_id': 1, 'return_date': '2023-02-24'
            }),
        ])
    )

    # # case 5：delete book
    # transactions.append(
    #     Transaction(transaction_id=5, type='T5', payload={}, hops=[
    #         Hop(hop_id=1, node=9001, action='delete_book', parameters={
    #             'book_id': 1
    #         })
    #     ])
    # )

    # # case 6：query user information
    # transactions.append(
    #     Transaction(transaction_id=6, type='T6', payload={}, hops=[
    #         Hop(hop_id=1, node=9002, action='query_user', parameters={
    #             'user_id': 1
    #         })
    #     ])
    # )

    # # case7：track user loans
    # transactions.append(
    #     Transaction(transaction_id=7, type='T7', payload={}, hops=[
    #         Hop(hop_id=1, node=9002, action='track_loans', parameters={
    #             'user_id': 1
    #         }),
    #         Hop(hop_id=2, node=9001, action='update_book_status', parameters={
    #             'book_id': 1, 'status': 'Borrowed'
    #         })
    #     ])
    # )

    threads = []
    for transaction in transactions:
        thread = threading.Thread(target=execute_use_case, args=(client, transaction))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()