import sqlite3

def initialize_db():
    print("Initializing database...")
    conn = sqlite3.connect('library_system.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS Books (
                        book_id INTEGER PRIMARY KEY,
                        title TEXT,
                        author TEXT,
                        publication_date DATE,
                        category TEXT,
                        status TEXT)''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS Loans (
                        loan_id INTEGER PRIMARY KEY,
                        book_id INTEGER,
                        user_id INTEGER,
                        borrow_date DATE,
                        return_date DATE,
                        due_date DATE,
                        FOREIGN KEY(book_id) REFERENCES Books(book_id),
                        FOREIGN KEY(user_id) REFERENCES Users(user_id))''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS Users (
                        user_id INTEGER PRIMARY KEY,
                        name TEXT,
                        email TEXT,
                        membership TEXT)''')
    
    conn.commit()
    cursor.close()
    conn.close()
    print("Complete...")

initialize_db()