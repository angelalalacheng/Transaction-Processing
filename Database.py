import sqlite3

def create_database(db_name, base_id):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS Books (
                        book_id INTEGER PRIMARY KEY,
                        title TEXT,
                        author TEXT,
                        publication_date DATE,
                        category TEXT,
                        status TEXT, 
                        loan_id INTEGER)''')

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
    
    init_users = ((base_id + 1, 'User 1', 'user1@example.com', db_name), 
                  (base_id + 2, 'User 2', 'user2@example.com', db_name), 
                  (base_id + 3, 'User 3', 'user3@example.com', db_name))
      
    init_books = ((base_id + 1, 'Book 1', 'Author 1', '2023-01-01', 'Fiction', 'Available'),
                  (base_id + 2, 'Book 2', 'Author 2', '2023-01-01', 'History', 'Available'),
                  (base_id + 3, 'Book 3', 'Author 3', '2023-01-01', 'Romance', 'Available'))
    
    cursor.executemany('''INSERT INTO 
                       Users (user_id, name, email, membership) 
                       VALUES (?, ?, ?, ?)''', init_users)
    cursor.executemany('''INSERT INTO 
                       Books (book_id, title, author, publication_date, category, status) 
                       VALUES (?, ?, ?, ?, ?, ?)''', init_books)

    conn.commit()
    conn.close()
    print(f"{db_name} created.")

def initialize_db():
    # Create three nodes
    create_database('Library A', 1000)
    create_database('Library B', 2000)
    create_database('Library C', 3000)


initialize_db()