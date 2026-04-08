from os import getenv
from shutil import copy2
from pathlib import Path
from typing import Literal
from oracledb import connect, DatabaseError
from dotenv import load_dotenv
from datetime import datetime as dt

load_dotenv()

DB_USER = getenv("DB_USER")
DB_PASS = getenv("DB_PASS")
DB_DSN = getenv("DB_DSN")


def tranferir_arquivos(origem: Path, tipo: Literal["anexo", "corpo"], id_rotina: int) -> None:
    """Copia o arquivo de origem para o diretório de rede correspondente ao tipo e à rotina."""
    diretorios = {
        'anexo': '//10.3.101.78/anexos',
        'corpo': '//10.3.101.78/corpos',
    }

    diretorio = diretorios.get(tipo)
    if not diretorio:
        raise ValueError(f"Tipo inválido: '{tipo}'. Use 'anexo' ou 'corpo'.")

    try:
        rotinas_dir = Path(fr'{diretorio}\rotina_{id_rotina}')
        rotinas_dir.mkdir(parents=True, exist_ok=True)
        print(f"Pasta '{rotinas_dir}' verificada/criada.")

        copy2(origem, rotinas_dir)
        print(f"Arquivo '{Path(origem).name}' copiado com sucesso para '{rotinas_dir}'.")

    except Exception as e:
        print(f"Erro ao copiar arquivo '{origem}': {e}")
        raise


class DB:
    def __connect(self):
        self.conn = connect(user=DB_USER, password=DB_PASS, dsn=DB_DSN)
        self.cursor = self.conn.cursor()

    def __disconnect(self):
        self.cursor.close()
        self.conn.close()

    def inserir_cadastro_rotina(
            self,
            nome: str,
            periodo: str,
            intervalo: int,
            dta_inicial: dt | None,
            dta_final: dt | None,
            consulta: str | None,
            tipo: str,
            ativo: str,
            destinatarios: list[str],
            corpos: dict | None = None,
            hiperlinks: dict | None = None,
            anexos: dict | None = None,
    ) -> int:
        """
        Insere o cadastro completo de uma rotina no banco de dados.
        Retorna o ID da rotina recém-criada.
        """
        # Garante dicionários vazios em vez de None para simplificar iteração
        corpos = corpos or {}
        hiperlinks = hiperlinks or {}
        anexos = anexos or {}

        self.__connect()
        try:
            # Cadastro dos dados básicos da rotina
            self.cursor.execute(
                "BEGIN p_pro_cadastrar_rotinas(:nome, :periodo, :intervalo, :dta_inicial, :dta_final, :consulta, :tipo, :ativo); END;",
                (nome, periodo, intervalo, dta_inicial, dta_final, consulta, tipo, ativo)
            )

            id_rotina: int = self.cursor.execute(
                "SELECT seq_cadastro_rotinas.currval FROM dual"
            ).fetchone()[0]

            # Cadastro dos destinatários
            self.cursor.executemany(
                "INSERT INTO email_rotinas VALUES (:1, :2, seq_email_rotinas.nextval)",
                [(id_rotina, email) for email in destinatarios]
            )

            sql_informativo = """
                INSERT INTO estrutura_informativo
                VALUES (
                    seq_estrurura_informativo.nextval,
                    :id_rotina,
                    :hiper_link,
                    :corpo,
                    :ordem,
                    :anexo
                )
            """

            # Inserção dos corpos
            for ordem, value in corpos.items():
                self.cursor.execute(sql_informativo, {
                    "id_rotina": id_rotina,
                    "hiper_link": hiperlinks.get(ordem),
                    "corpo": Path(value).name,
                    "ordem": ordem,
                    "anexo": None,
                })

            # Inserção dos anexos
            for value in anexos.values():
                self.cursor.execute(sql_informativo, {
                    "id_rotina": id_rotina,
                    "hiper_link": None,
                    "corpo": None,
                    "ordem": None,
                    "anexo": Path(value).name,
                })

            self.conn.commit()
            return id_rotina

        except Exception as e:
            self.conn.rollback()
            raise DatabaseError(f"Erro ao realizar o cadastro da rotina: {e}") from e

        finally:
            self.__disconnect()
