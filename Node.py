import socket
import threading
import pickle
import sqlite3
import argparse
from SCgraph import DependencyGraph
from Transaction import Transaction
from Hop import Hop

class Node:
    def __init__(self, host, port, other_nodes):
        self.host = host
        self.port = port
        self.other_nodes = other_nodes  # 其他节点的信息
        self.dependency_graph = DependencyGraph()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.host, self.port))
        self.socket.listen(5)
        print(f"Node started at {self.host}:{self.port}")

    def handle_connection(self, conn, addr):
        try:
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                received_object = pickle.loads(data)
                if isinstance(received_object, Transaction):
                    response = self.process_transaction(received_object)
                elif isinstance(received_object, Hop):
                    response = self.process_hop(received_object)
                else:
                    response = {"status": "Error", "message": "Invalid object received"}
                conn.sendall(pickle.dumps(response))
        except (ConnectionError, EOFError, pickle.UnpicklingError) as e:
            print(f"Error during connection handling: {e}")
        finally:
            conn.close()


    def process_transaction(self, transaction):
        conn = sqlite3.connect('library_system.db')
        conn.cursor()
        
        results = []
        for hop in transaction.hops:
            if hop.node == self.port:
                result = self.execute_action(hop.action, hop.parameters)
                if result.get("status") == "Failed":
                    return result
                results.append(result)
            else:
                result = self.send_to_other_node(hop)
                if result.get("status") == "Failed":
                    return result
                results.append(result)
            
            # 添加依赖关系到内存中的图中
            self.dependency_graph.add_edge(transaction.transaction_id, hop.hop_id)
        
        conn.commit()
        conn.close()

        # 检查是否存在循环
        if self.dependency_graph.is_cyclic():
            return {"status": "Failed", "message": "SC Cycle detected"}
        return results
    
    def process_hop(self, hop):
        if hop.node == self.port:
            return self.execute_action(hop.action, hop.parameters)
        else:
            return self.send_to_other_node(hop)

    def send_to_other_node(self, hop):
        print(f"## Sending hop {hop} to {hop.node}")
        node_info = self.other_nodes[hop.node]
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((node_info['host'], node_info['port']))
            request = pickle.dumps(hop)
            s.sendall(request)
            response = s.recv(1024)
        return pickle.loads(response)

    def execute_action(self, action, parameters):
        conn = sqlite3.connect('library_system.db')
        cursor = conn.cursor()
        
        if action == 'add_book':
            book_id = parameters['book_id']
            title = parameters['title']
            author = parameters['author']
            publication_date = parameters['publication_date']
            category = parameters['category']
            status = parameters['status']
            
            # check if book already exists
            cursor.execute("SELECT * FROM Books WHERE book_id = ?", (book_id,))
            book = cursor.fetchone()
            if book:
                conn.close()
                return {"status": "Failed", "message": f"Book {book_id} already exists"}
            else:
                cursor.execute("INSERT INTO Books (book_id, title, author, publication_date, category, status) VALUES (?, ?, ?, ?, ?, ?)", 
                               (book_id, title, author, publication_date, category, status))
                conn.commit()
                conn.close()
                return {"status": "Success", "message": f"Book {book_id} added"}
        
        elif action == 'delete_book':
            book_id = parameters['book_id']
            cursor.execute("DELETE FROM Books WHERE book_id = ?", (book_id,))
            conn.commit()
            conn.close()
            return {"status": "Success", "message": f"Book {book_id} deleted"}
        
        elif action == 'query_book':
            book_id = parameters['book_id']
            cursor.execute("SELECT * FROM Books WHERE book_id = ?", (book_id,))
            book_info = cursor.fetchone()
            conn.close()
            if not book_info:
                return {"status": "Failed", "message": f"Book {book_id} not found"}
            return {"status": "Success", "message": f"Book {book_id} exists"}
        
        elif action == 'borrow_book':
            book_id = parameters['book_id']
            user_id = parameters['user_id']
            borrow_date = parameters['borrow_date']
            due_date = parameters['due_date']
            cursor.execute("INSERT INTO Loans (book_id, user_id, borrow_date, due_date) VALUES (?, ?, ?, ?)", 
                           (book_id, user_id, borrow_date, due_date))
            cursor.execute("UPDATE Books SET status = 'Borrowed' WHERE book_id = ?", (book_id,))
            conn.commit()
            conn.close()
            return {"status": "Success", "message": f"Book {book_id} borrowed by user {user_id}"}
        
        elif action == 'return_book':
            loan_id = parameters['loan_id']
            return_date = parameters['return_date']
            cursor.execute("UPDATE Loans SET return_date = ? WHERE loan_id = ?", (return_date, loan_id))
            cursor.execute("UPDATE Books SET status = 'Available' WHERE book_id = (SELECT book_id FROM Loans WHERE loan_id = ?)", (loan_id,))
            conn.commit()
            conn.close()
            return {"status": "Success", "message": f"Loan {loan_id} closed"}
        
        elif action == 'query_book_status':
            book_id = parameters['book_id']
            status = parameters['status']
            cursor.execute("UPDATE Books SET status = ? WHERE book_id = ?", (status, book_id))
            conn.commit()
            conn.close()
            return {"status": f"{status}", "message": f"Book {book_id} is {status}"}

        elif action == 'add_user':
            user_id = parameters['user_id']
            name = parameters['name']
            email = parameters['email']
            membership = parameters['membership']

            cursor.execute("SELECT * FROM Users WHERE user_id = ?", (user_id,))
            user = cursor.fetchone()
            if user:
                conn.close()
                return {"status": "Failed", "message": f"User {user_id} already exists"}
            else:
                cursor.execute("INSERT INTO Users (user_id, name, email, membership) VALUES (?, ?, ?, ?)", 
                            (user_id, name, email, membership))
                conn.commit()
                conn.close()
                return {"status": "Success", "message": f"User {user_id} added"}
        
        elif action == 'query_user':
            user_id = parameters['user_id']
            cursor.execute("SELECT * FROM Users WHERE user_id = ?", (user_id,))
            user_info = cursor.fetchone()
            conn.close()
            if not user_info:
                return {"status": "Failed", "message": f"User {user_id} not the memeber"}
            return {"status": "Success", "data": user_info}

        elif action == 'check_loan':
            user_id = parameters['user_id']
            loan_id = parameters['loan_id'] 
            cursor.execute("SELECT * FROM Loans WHERE user_id = ? and loan_id = ?", (user_id, loan_id))
            loan_info = cursor.fetchone()
            conn.close()
            if not loan_info:
                return {"status": "Failed", "message": f"Loan {loan_id} not found"}
            return {"status": "Success", "data": loan_info}
        
        # 添加其他操作处理逻辑
        conn.close()
        return {"status": "Unknown action"}

    def start(self):
        while True:
            conn, addr = self.socket.accept()
            threading.Thread(target=self.handle_connection, args=(conn, addr)).start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start a node.")
    parser.add_argument('--port', type=int, required=True, help='The port number for the node.')
    args = parser.parse_args()

    other_nodes = {
        9001: {'host': 'localhost', 'port': 9001},
        9002: {'host': 'localhost', 'port': 9002},
        9003: {'host': 'localhost', 'port': 9003}
    }
    node = Node('localhost', args.port, other_nodes)
    node.start()