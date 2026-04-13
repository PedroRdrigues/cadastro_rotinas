from os import getenv
from shutil import copy2
from pathlib import Path
from typing import Literal, Any
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
        'anexo': '//10.3.101.78/anexos2',
        'corpo': '//10.3.101.78/corpos2',
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
        corpos: dict[Any, Any] | None = None,
        hiperlinks: dict[Any, Any] | None = None,
        anexos: dict[Any, Any] | None = None,
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

    def buscar_rotinas(
        self,
        nome: str | None = None,
        id_rotina: int | None = None,
    ) -> list[dict]:
        """
        Busca rotinas pelo nome (LIKE) e/ou pelo ID exato.
        Retorna uma lista de dicionários com os dados completos de cada rotina,
        incluindo destinatários e estrutura informativo.
        """
        self.__connect()
        try:
            condicoes = []
            params = {}

            if nome:
                condicoes.append("UPPER(r.nome) LIKE UPPER(:nome)")
                params["nome"] = f"%{nome}%"
            if id_rotina is not None:
                condicoes.append("r.id_rotina = :id_rotina")
                params["id_rotina"] = id_rotina

            where = f"WHERE {' AND '.join(condicoes)}" if condicoes else ""

            sql_rotinas = f"""
                SELECT r.id_rotina,
                       r.nome,
                       r.periodo,
                       r.intervalo,
                       r.dta_inicial,
                       r.dta_final,
                       r.sql_rotina,
                       r.tipo,
                       r.ativo
                  FROM cadastro_rotinas r
                {where}
                ORDER BY r.id_rotina
            """
            self.cursor.execute(sql_rotinas, params)
            colunas_rotina = [c[0].lower() for c in self.cursor.description]
            rotinas_raw = self.cursor.fetchall()

            if not rotinas_raw:
                return []

            rotinas = []
            for row in rotinas_raw:
                rotina = dict(zip(colunas_rotina, row))
                rid = rotina["id_rotina"]

                # Destinatários
                self.cursor.execute(
                    "SELECT email FROM email_rotinas WHERE id_rotina = :1", (rid,)
                )
                rotina["destinatarios"] = [r[0] for r in self.cursor.fetchall()]

                # Estrutura informativo (corpos e anexos)
                self.cursor.execute(
                    """SELECT hiper_link, corpo, ordem, anexo
                         FROM estrutura_informativo
                        WHERE id_rotina = :1
                        ORDER BY ordem NULLS LAST""",
                    (rid,)
                )
                corpos = {}
                links = {}
                anexos = {}
                idx_anexo = 1
                for hiper_link, corpo, ordem, anexo in self.cursor.fetchall():
                    if corpo is not None:
                        corpos[int(ordem)] = corpo
                        if hiper_link:
                            links[int(ordem)] = hiper_link
                    elif anexo is not None:
                        anexos[idx_anexo] = anexo
                        idx_anexo += 1

                rotina["corpos"] = corpos
                rotina["links"] = links
                rotina["anexos"] = anexos
                rotinas.append(rotina)

            return rotinas

        except Exception as e:
            raise DatabaseError(f"Erro ao buscar rotinas: {e}") from e

        finally:
            self.__disconnect()

    def atualizar_rotina(
        self,
        id_rotina: int,
        nome: str,
        periodo: str,
        intervalo: int,
        dta_inicial: dt | None,
        dta_final: dt | None,
        consulta: str | None,
        tipo: str,
        ativo: str,
        destinatarios: list[str],
        corpos: dict[Any, Any] | None = None,
        hiperlinks: dict[Any, Any] | None = None,
        anexos: dict[Any, Any] | None = None,
    ) -> None:
        """Atualiza todos os dados de uma rotina existente pelo seu ID."""
        corpos = corpos or {}
        hiperlinks = hiperlinks or {}
        anexos = anexos or {}

        self.__connect()
        try:
            self.cursor.execute(
                """UPDATE cadastro_rotinas
                      SET nome        = :nome,
                          periodo     = :periodo,
                          intervalo   = :intervalo,
                          dta_inicial = :dta_inicial,
                          dta_final   = :dta_final,
                          sql_rotina  = :sql,
                          tipo        = :tipo,
                          ativo       = :ativo
                    WHERE id_rotina   = :id_rotina""",
                {
                    "nome": nome, "periodo": periodo, "intervalo": intervalo,
                    "dta_inicial": dta_inicial, "dta_final": dta_final,
                    "sql": consulta, "tipo": tipo, "ativo": ativo,
                    "id_rotina": id_rotina,
                }
            )

            # Recria destinatários
            self.cursor.execute(
                "DELETE FROM email_rotinas WHERE id_rotina = :1", (id_rotina,)
            )
            self.cursor.executemany(
                "INSERT INTO email_rotinas VALUES (:1, :2, seq_email_rotinas.nextval)",
                [(id_rotina, email) for email in destinatarios]
            )

            # Recria estrutura informativo
            self.cursor.execute(
                "DELETE FROM estrutura_informativo WHERE id_rotina = :1", (id_rotina,)
            )

            sql_informativo = """
                INSERT INTO estrutura_informativo
                VALUES (
                    seq_estrurura_informativo.nextval,
                    :id_rotina, :hiper_link, :corpo, :ordem, :anexo
                )
            """
            for ordem, value in corpos.items():
                self.cursor.execute(sql_informativo, {
                    "id_rotina": id_rotina,
                    "hiper_link": hiperlinks.get(ordem),
                    "corpo": Path(value).name if Path(value).exists() else value,
                    "ordem": ordem,
                    "anexo": None,
                })
            for value in anexos.values():
                self.cursor.execute(sql_informativo, {
                    "id_rotina": id_rotina,
                    "hiper_link": None,
                    "corpo": None,
                    "ordem": None,
                    "anexo": Path(value).name if Path(value).exists() else value,
                })

            self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            raise DatabaseError(f"Erro ao atualizar a rotina: {e}") from e

        finally:
            self.__disconnect()