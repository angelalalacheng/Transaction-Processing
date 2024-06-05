import socket
import sqlite3
import threading
import pickle
from Transaction import Transaction

class Server:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.host, self.port))
        self.socket.listen(5)
        print("Server started and listening on", (self.host, self.port))


    def handle_client(self, conn, addr):
        print(f"Connected by {addr}")

        while True:
            data = conn.recv(1024)
            if not data:
                break
            received_transaction = pickle.loads(data)
            self.process_transaction(conn, received_transaction)

        conn.close()


    def process_transaction(self, conn, transaction):
        return_value = None

        print({"status": "Begin", "message": f"Transaction{transaction.transaction_id} begin"})
    
        first_hop_result = self.exec_first_hop(conn, transaction.transaction_id, transaction.hops[0])
        if first_hop_result.get("status") == "Abort":
            return None
        else:
            return_value = first_hop_result.get("return_value", None)

        for hop in transaction.hops[1:]:
            result = self.execute_action(hop.node, hop.action, hop.parameters, return_value)
            return_value = result.get("return_value", None)

            
        print({"status": "Complete", "message": f"Transaction{transaction.transaction_id} complete"})
    

    def exec_first_hop(self, conn, t_id, hop):
        result = self.execute_action(hop.node, hop.action, hop.parameters)

        transaction_result = {}
        if result.get("status") == "Failed":
            transaction_result = {"status": "Abort", "message": f"Transaction{t_id} abort"}
        else:
            transaction_result = {"status": "Commit", "message": f"Transaction{t_id} committed"}
        
        conn.sendall(pickle.dumps(transaction_result))

        transaction_result.update({"return_value": result.get("return_value", None)})
        return transaction_result


    def execute_action(self, node, action, parameters, return_value = None):
        conn_db = sqlite3.connect(node)
        cursor = conn_db.cursor()

        result = {}
        if action == 'add_book':
            book_id = parameters['book_id']
            title = parameters['title']
            author = parameters['author']
            publication_date = parameters['publication_date']
            category = parameters['category']
            status = parameters['status']
            
            # check if the book already exists
            if self.book_exist(cursor, book_id):
                result = {"status": "Failed", "message": f"Book {book_id} already exists"}
            else:
                cursor.execute("INSERT INTO Books (book_id, title, author, publication_date, category, status) VALUES (?, ?, ?, ?, ?, ?)", 
                               (book_id, title, author, publication_date, category, status))
                conn_db.commit()
                result = {"status": "Success", "message": f"Book {book_id} added"}
        
        elif action == 'add_user':
            user_id = parameters['user_id']
            name = parameters['name']
            email = parameters['email']
            membership = parameters['membership']

            # check if the user already exists
            if self.user_exist(cursor, user_id):
                result = {"status": "Failed", "message": f"User {user_id} already exists"}
            else:
                cursor.execute("INSERT INTO Users (user_id, name, email, membership) VALUES (?, ?, ?, ?)", 
                            (user_id, name, email, membership))
                conn_db.commit()
                result = {"status": "Success", "message": f"User {user_id} added"}

        elif action == 'delete_book':
            book_id = parameters['book_id']

            cursor.execute("DELETE FROM Books WHERE book_id = ?", (book_id,))
            conn_db.commit()
            if cursor.rowcount == 0:
                result = {"status": "Failed", "message": f"Book {book_id} doesn't exist"}
            else:
                result = {"status": "Success", "message": f"Book {book_id} deleted"}
        
        elif action == 'borrow_book':
            book_id = parameters['book_id']
            user_id = parameters['user_id']
            borrow_date = parameters['borrow_date']
            due_date = parameters['due_date']

            if self.book_available(cursor, book_id):
                cursor.execute("INSERT INTO Loans (book_id, user_id, borrow_date, due_date) VALUES (?, ?, ?, ?)", 
                            (book_id, user_id, borrow_date, due_date))
                loan_id = cursor.lastrowid
                cursor.execute("UPDATE Books SET status = 'Borrowed', loan_id = ? WHERE book_id = ?", (loan_id, book_id))
                conn_db.commit()
                result = {"status": "Success", "message": f"User {user_id} borrowed book {book_id}", "return_value": {"loan_id": loan_id}}
            else:
                result = {"status": "Failed", "message": f"Book {book_id} is not available"}

        elif action == 'add_loan':
            book_id = parameters['book_id']
            user_id = parameters['user_id']
            borrow_date = parameters['borrow_date']
            due_date = parameters['due_date']
            loan_id = return_value.get("loan_id")

            cursor.execute("INSERT INTO Loans (loan_id, book_id, user_id, borrow_date, due_date) VALUES (?, ?, ?, ?, ?)", 
                        (loan_id, book_id, user_id, borrow_date, due_date))
            conn_db.commit()
            result = {"status": "Success", "message": f"Loan {loan_id} added", "return_value": {"loan_id": loan_id}}

        elif action == 'return_book':
            book_id = parameters['book_id']
            return_date = parameters['return_date']

            cursor.execute("SELECT loan_id from Books WHERE book_id = ?", (book_id,))
            loan_id = cursor.fetchone()[0]

            cursor.execute("UPDATE Books SET status = 'Available', loan_id = NULL WHERE book_id = ?", (book_id,))

            cursor.execute("UPDATE Loans SET return_date = ? WHERE loan_id = ?", (return_date, loan_id))
            
            conn_db.commit()
            result = {"status": "Success", "message": f"Book {book_id} is returned", "return_value": {"loan_id": loan_id}}
        
        elif action == 'update_loan':
            return_date = parameters['return_date']
            loan_id = return_value.get("loan_id")

            cursor.execute("UPDATE Loans SET return_date = ? WHERE loan_id = ?", (return_date, loan_id))
            conn_db.commit()
            result = {"status": "Success", "message": f"Loan {loan_id} closed", "return_value": {"loan_id": loan_id}}
        
        elif action == 'query_user':
            user_id = parameters['user_id']

            cursor.execute("SELECT * FROM Users WHERE user_id = ?", (user_id,))
            user_info = cursor.fetchone()
            if not user_info:
                result = {"status": "Failed", "message": f"User {user_id} not the member in {node}"}
            result = {"status": "Success", "data": user_info}
        
        elif action == 'track_loans':
            cursor.execute("SELECT * FROM Loans WHERE loan_id IN (SELECT loan_id FROM Books WHERE status = 'Borrowed')")
            unreturned_loans = cursor.fetchall()
            if not unreturned_loans:
                result = {"status": "Failed", "message": f"All books in {node} are available"}
            else:
                result = {"status": "Success", "data": unreturned_loans}
        
        else:
            result = {"status": "Unknown action"}
        
        conn_db.close()
        print(result)
        return result


    def user_exist(self, cursor, user_id):
        cursor.execute("SELECT * FROM Users WHERE user_id = ?", (user_id,))
        user_info = cursor.fetchone()
        return True if user_info else False
        

    def book_exist(self, cursor, book_id):
        cursor.execute("SELECT * FROM Books WHERE book_id = ?", (book_id,))
        book_info = cursor.fetchone()
        return True if book_info else False
    

    def book_available(self, cursor, book_id):
        cursor.execute("SELECT status FROM Books WHERE book_id = ?", (book_id,))
        book_status = cursor.fetchone()[0]
        return True if book_status == 'Available' else False


    def start(self):
        while True:
            conn, addr = self.socket.accept()
            threading.Thread(target=self.handle_client, args=(conn, addr)).start()


if __name__ == "__main__":
    HOST = 'localhost'
    PORT = 9000

    server = Server(HOST, PORT)
    server.start()
