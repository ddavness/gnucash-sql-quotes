import psycopg

class PgSQLDriver():
    cnx = None

    def __init__(self, host = "localhost", database = None, user = None, pw = None, port = 5432) -> None:
        super().__init__()
        userspec = f"{user}:{pw}@" if user is not None and pw is not None \
            else (f"{user}@" if user is not None else "")
        if database is None:
            raise ValueError("No database specified!")

        self.url = f"postgresql://{userspec}{host}:{port}/{database}"
    
    def __enter__(self):
        self.cnx = psycopg.connect(self.url)
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
