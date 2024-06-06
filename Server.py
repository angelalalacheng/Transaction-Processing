import socket
import sqlite3
import threading
import pickle

class Server:
    def __init__(self, host, port, db_name):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self.host, self.port))
        self.socket.listen(5)
        print(f"Server started and listening on {self.host}:{self.port} for database {self.db_name}")

    def handle_client(self, conn, addr):
        raise NotImplementedError("Must be implemented by subclass.")

    def execute_action(self, node, action, parameters, return_value = None):
        conn_db = sqlite3.connect(self.db_name)
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

            if self.book_exist(cursor, book_id):
                if self.book_available(cursor, book_id): 
                    cursor.execute("INSERT INTO Loans (book_id, user_id, borrow_date, due_date) VALUES (?, ?, ?, ?)", 
                                (book_id, user_id, borrow_date, due_date))
                    loan_id = cursor.lastrowid
                    cursor.execute("UPDATE Books SET status = 'Borrowed', loan_id = ? WHERE book_id = ?", (loan_id, book_id))
                    conn_db.commit()
                    result = {"status": "Success", "message": f"User {user_id} borrowed book {book_id}", "return_value": {"loan_id": loan_id}}
                else:
                    result = {"status": "Failed", "message": f"Book {book_id} is not available"}
            else:
                result = {"status": "Failed", "message": f"Book {book_id} is not exist"}

        elif action == 'add_loan':
            book_id = parameters['book_id']
            user_id = parameters['user_id']
            borrow_date = parameters['borrow_date']
            due_date = parameters['due_date']
            loan_id = return_value.get("loan_id")
            print(f"Add Loans at {self.db_name} / {self.port}")
            cursor.execute("INSERT INTO Loans (loan_id, book_id, user_id, borrow_date, due_date) VALUES (?, ?, ?, ?, ?)", 
                        (loan_id, book_id, user_id, borrow_date, due_date))
            conn_db.commit()
            result = {"status": "Success", "message": f"Loan {loan_id} added", "return_value": {"loan_id": loan_id}}

        elif action == 'return_book':
            book_id = parameters['book_id']
            return_date = parameters['return_date']

            cursor.execute("SELECT loan_id from Books WHERE book_id = ?", (book_id,))
            loan_id = cursor.fetchone()[0]

            if not loan_id:
                result = {"status": "Failed", "message": f"Book {book_id} is not borrowed"}

            elif self.book_available(cursor, book_id):
                result = {"status": "Failed", "message": f"Book {book_id} is now available"}

            else:
                cursor.execute("UPDATE Books SET status = 'Available', loan_id = NULL WHERE book_id = ?", (book_id,))

                cursor.execute("UPDATE Loans SET return_date = ? WHERE loan_id = ?", (return_date, loan_id))
                
                conn_db.commit()
                result = {"status": "Success", "message": f"Book {book_id} is returned", "return_value": {"loan_id": loan_id}}
        
        elif action == 'update_loan':
            return_date = parameters['return_date']
            loan_id = return_value.get("loan_id")

            if self.loan_exist(cursor, loan_id):
                cursor.execute("UPDATE Loans SET return_date = ? WHERE loan_id = ?", (return_date, loan_id))
                conn_db.commit()
                result = {"status": "Success", "message": f"Loan {loan_id} closed", "return_value": {"loan_id": loan_id}}

            else:
                result = {"status": "Failed", "message": f"Loan doesn't exist"}
        
        elif action == 'query_user':
            user_id = parameters['user_id']

            cursor.execute("SELECT * FROM Users WHERE user_id = ?", (user_id,))
            user_info = cursor.fetchone()
            if not user_info:
                result = {"status": "Failed", "message": f"User {user_id} not the member in {self.db_name}"}
            result = {"status": "Success", "data": user_info}
        
        elif action == 'track_loans':
            cursor.execute("SELECT * FROM Loans WHERE loan_id IN (SELECT loan_id FROM Books WHERE status = 'Borrowed')")
            unreturned_loans = cursor.fetchall()
            if not unreturned_loans:
                result = {"status": "Success", "message": f"All books in {self.db_name} are available"}
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
    
    def loan_exist(self, cursor, loan_id):
        cursor.execute("SELECT * FROM Loans WHERE loan_id = ?", (loan_id,))
        loan_info = cursor.fetchone()
        return True if loan_info else False


    def start(self):
        while True:
            conn, addr = self.socket.accept()
            threading.Thread(target=self.handle_client, args=(conn, addr)).start()


class BaseServer(Server):
    def handle_client(self, conn, addr):
        print(f"Connected by {addr}")
        print(f"At Database: {self.db_name}, Port: {self.port}, Host: {self.host}")

        while True:
            try:
                data = conn.recv(1024)
                if not data:
                    break
                received_data = pickle.loads(data)
                hop = received_data['hop']
                return_value = received_data.get('return_value', None)

                result = self.execute_action(hop.node, hop.action, hop.parameters, return_value)
                conn.sendall(pickle.dumps(result))
            except Exception as e:
                print(f"Error handling client {addr}: {e}")
                break

        conn.close()


class OriginOrderServer(Server):
    def __init__(self, host, port, db_name):
        super().__init__(host, port, db_name)
        self.sequence_number = 0
        self.sequence_numbers = {}

    def get_sequence_number(self):
        self.sequence_number += 1
        return self.sequence_number
    
    def handle_client(self, conn, addr):
        print(f"Connected by {addr}")
        print(f"At Database: {self.db_name}, Port: {self.port}, Host: {self.host}")

        while True:
            try:
                data = conn.recv(1024)
                if not data:
                    break
                received_data = pickle.loads(data)

                # Case 1: Get sequence numbe
                if 'get_sequence_number' in received_data:
                    sequence_number = self.get_sequence_number()
                    conn.sendall(pickle.dumps(sequence_number))
                    continue

                # Case 2: Process hop data
                hop = received_data['hop']
                return_value = received_data.get('return_value', None)
                sequence_number = received_data.get('sequence_number')

                if hop.node not in self.sequence_numbers:
                    self.sequence_numbers[hop.node] = []

                self.sequence_numbers[hop.node].append((sequence_number, hop, return_value))
                self.sequence_numbers[hop.node].sort()  # Ensure hops are processed in order

                while self.sequence_numbers[hop.node]:
                    seq_num, hop, return_value = self.sequence_numbers[hop.node].pop(0)
                    result = self.execute_action(hop.node, hop.action, hop.parameters, return_value)
                    conn.sendall(pickle.dumps(result))
            
            except Exception as e:
                print(f"Error handling client {addr}: {e}")
                break

        conn.close()


if __name__ == "__main__":
    HOST = 'localhost'
    PORTS = [9000, 9001, 9002]
    DB_NAMES = ['Library A', 'Library B', 'Library C']

    servers = []
    for port, db_name in zip(PORTS, DB_NAMES):
        server = BaseServer(HOST, port, db_name)
        threading.Thread(target=server.start).start()
        servers.append(server)