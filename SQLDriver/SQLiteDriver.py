import sqlite3

class SQLiteDriver():
    target = None

    def __init__(self, file = ":memory:") -> None:
        self.target = file
    
    def __enter__(self):
        self.cnx = sqlite3.connect(self.target)
        return self

    def __exit__(self, _et, _ev, _tb):
        self.cnx.commit()
        self.cnx.close()
        self.cnx = None

    def execute(self, query = "", params = tuple()) -> list:
        if self.cnx:
            raise ValueError("Connection not established yet!")
        
        with self.cnx.cursor() as c:
            c.execute(query, params)
            return c.fetchall()
