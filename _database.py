"""Manipulação de banco de dados Oracle utilizando python-oracledb."""

import logging
from os import getenv
from typing import Any, Dict, List, Optional

try:
    import oracledb
    from oracledb import create_pool, InterfaceError
except ImportError:
    logging.error("Biblioteca 'oracledb' não instalada. Execute: pip install oracledb")

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    logging.warning("Biblioteca 'python-dotenv' não instalada. Certifique-se de que as variáveis de ambiente estão configuradas.")

# Configurações do Banco vindas do .env
DB_USER = getenv("DB_USER")
DB_PASS = getenv("DB_PASS")
DB_DSN = getenv("DB_DSN")

class DB:
    def __init__(self):
        """Inicializa o Pool de Conexões Oracle."""
        try:
            # O Pool gerencia as conexões automaticamente, evitando 'Timed Out'
            self.pool = create_pool(
                user=DB_USER,
                password=DB_PASS,
                dsn=DB_DSN,
                min=2,
                max=10,
                increment=1
            )
            logging.info("Pool de conexões Oracle estabelecido com sucesso.")
        except Exception as e:
            logging.critical(f"Falha crítica ao conectar no Banco: {e}")
            raise

    def consultar(self, query: str, params: Optional[List] = None) -> Dict[str, Any]:
        """
        Executa uma consulta e retorna um dicionário com:
        - 'data': Lista de registros (cada registro é uma lista)
        - 'description': Metadados das colunas
        """
        try:
            with self.pool.acquire() as connection:
                with connection.cursor() as cursor:
                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)

                    # description contém o nome das colunas e tipos
                    description = cursor.description
                    # Converte tuplas em listas para manter compatibilidade com seu processamento de Excel
                    data = [list(row) for row in cursor.fetchall()]

                    return {
                        "data": data,
                        "description": description
                    }
        except Exception as e:
            logging.error(f"Erro ao executar consulta SQL: {e}")
            raise Exception(f"Erro ao executar consulta SQL: {e}")

    def executar(self, sql: str, params: Optional[List] = None) -> bool:
        """Executa comandos de INSERT, UPDATE, DELETE ou PROCEDURE."""
        try:
            with self.pool.acquire() as connection:
                with connection.cursor() as cursor:
                    if params:
                        cursor.execute(sql, params)
                    else:
                        cursor.execute(sql)
                    connection.commit()
                    return True
        except Exception as e:
            logging.error(f"Erro ao executar comando SQL (Commit cancelado): {e}")
            return False