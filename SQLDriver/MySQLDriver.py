import mysql.connector

class MySQLDriver():
    cnx = None
    data = dict()

    def __init__(self, host = "localhost", database = None, user = None, pw = None, port = 3306) -> None:
        if user is not None and user.strip() != "" and pw is not None and pw.strip() != "":
            self.data["user"] = user
            self.data["password"] = pw
        elif user is not None and user.strip() != "":
            self.data["user"] = user
        elif pw is not None and pw.strip() != "":
            raise ValueError("When specifying a password, the username is required!")

        if database is None or database.strip() == "":
            raise ValueError("No database specified!")

        self.data["host"] = host
        self.data["port"] = port
        self.data["database"] = database

    def __enter__(self):
        self.cnx = mysql.connector.connect(**self.data)
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
