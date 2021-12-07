import psycopg

class PgSQLDriver():
    cnx = None

    def __init__(self, host = "localhost", database = None, user = None, pw = None, port = 5432) -> None:
        if host is None:
            host = "localhost"
        if port is None:
            port = 5432
        
        userspec = ""
        if user is not None and user.strip() != "" and pw is not None and pw.strip() != "":
            userspec = f"{user}:{pw}@"
        elif user is not None and user.strip() != "":
            userspec = f"{user}@"
        elif pw is not None and pw.strip() != "":
            raise ValueError("When specifying a password, the username is required!")

        if database is None or database.strip() == "":
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
        if self.cnx is None:
            raise ValueError("Connection not established yet!")
        
        with self.cnx.cursor() as c:
            c.execute(query, params)
            return c.fetchall()
