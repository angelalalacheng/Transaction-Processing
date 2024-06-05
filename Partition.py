class PartitionStrategy:
    def __init__(self, servers):
        self.servers = servers

    def get_server_for_user(self, user_id):
        # 根據用戶ID範圍分區
        if user_id < 2000:
            return 'Library A'
        elif user_id < 3000:
            return 'Library B'
        else:
            return 'Library C'

    def get_server_for_book(self, book_id):
        # 根據書籍ID範圍分區
        if book_id < 2000:
            return 'Library A'
        elif book_id < 3000:
            return 'Library B'
        else:
            return 'Library C'