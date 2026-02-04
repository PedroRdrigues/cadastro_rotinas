from sqlite3 import connect

# Classe para manipular o db
class DB:
    def __init__(self, host:str):
        self.host = host
        try:
            self.conn = connect(self.host)
            self.cursor = self.conn.cursor()

        except Exception as e:
            print(f"erro: {e}")


    def close_db(self):  # Fechar a conexão e o cursor
        self.conn.close()

    def consultar(self, sql:str) -> list | None:
        result = self.cursor.execute(sql)
        dados = [i for i in result]
        self.close_db()

        return dados

    def executar(self, sql:str) -> None:
        self.cursor.execute(sql)
        self.conn.commit()
        self.close_db()


if __name__ == "__main__":
    db = DB("C:\\Dev\\python\\cadastro_rotinas\\rotinas.sqlite")
    db.executar()

