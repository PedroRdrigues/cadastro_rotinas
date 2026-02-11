"""Manipulação de databese oracle com Oracledb."""

from os import getenv

try:
    from oracledb import create_pool, InterfaceError
except ImportError as e:
    print("Oracledb não instalado")

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError as e:
    print("dotenv não Instalado.")



# Configurações do Banco (Coloque as suas credenciais aqui)
DB_USER = getenv("DB_USER")
DB_PASS = getenv("DB_PASS")
DB_DSN = getenv("DB_DSN")


class DB:
    def __init__(self):
        # Cria o POOL de conexões apenas UMA vez na inicialização
        try:
            self.pool = create_pool(
                user=DB_USER,
                password=DB_PASS,
                dsn=DB_DSN,
                min=2,
                max=10,
                increment=1
            )
            print("Pool de conexões criado com sucesso.")
        except Exception as e:
            print("Erro na comunicação com o Banco:", e)
            raise Exception("Erro na comunicação com o Banco:", e)


    def consultar(self, query, params:list=None) -> list:
        # Pega uma conexão exclusiva do pool para esta execução
        with self.pool.acquire() as connection:
            with connection.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                # Fetchall retorna tuplas, convertemos para lista conforme seu código original
                return [list(row) for row in cursor.fetchall()]

    def executar(self, sql, params:list=None) -> None:
        with self.pool.acquire() as connection:
            with connection.cursor() as cursor:
                if params:
                    cursor.execute(sql, params)
                else:
                    cursor.execute(sql)
                connection.commit()
