from _database import DB, InterfaceError
from _emails import Email
from _utils import notify_error

from dataclasses import dataclass
from datetime import datetime as dt
from pathlib import Path
from threading import Thread
from typing import List, Optional, Any
from unicodedata import category, normalize

from atexit import register
from msvcrt import locking, LK_NBLCK
from os import getpid, _exit, getenv
from time import sleep
import logging

# Dependências externas
try:
    from openpyxl import Workbook
    from apscheduler.schedulers.blocking import BlockingScheduler
except ImportError as e:
    logging.error(f"Dependência faltando: {e}")



@dataclass
class RoutineData:
    """Estrutura para mapear os dados da rotina do banco."""
    id: int
    nome: str
    periodo: str
    intervalo: int
    dta_inicial: dt
    dta_proxima: Optional[dt]
    dta_final: Optional[dt]
    sql: Optional[str]
    tipo: str

    @classmethod
    def from_row(cls, row):
        return cls(
            id=row[0], nome=row[1], periodo=row[2], intervalo=row[3],
            dta_inicial=row[4], dta_proxima=row[5], dta_final=row[6],
            sql=str(row[7]).upper(), tipo=row[10]
        )


class RoutineService(DB):
    def __init__(self):
        super().__init__()
        self.base_path = Path.cwd()
        self.lock_file_path = self.base_path / "service.lock"
        self.lock_handle = None
        register(self.release_lock)

    def release_lock(self):
        if self.lock_handle:
            try:
                self.lock_handle.close()
                logging.info("--- [ Trava de arquivo liberada ] ---\n")
            except Exception:
                pass

    def acquire_lock(self):
        """Garante instância única do serviço."""
        for _ in range(5):
            try:
                self.lock_handle = open(self.lock_file_path, "w")
                locking(self.lock_handle.fileno(), LK_NBLCK, 1)
                self.lock_handle.write(str(getpid()))
                self.lock_handle.flush()
                logging.info(f"Lock adquirido (PID: {getpid()})")
                return True
            except (OSError, IOError):
                logging.warning("Outra instância em execução. Tentando novamente...")
                sleep(1)

        logging.error("Não foi possível adquirir o lock. Encerrando.")
        _exit(0)

    def run(self):
        self.acquire_lock()
        scheduler = BlockingScheduler()

        # Configuração do Job
        scheduler.add_job(
            self.check_routines,
            'cron',
            second='0',
            misfire_grace_time=15,
            coalesce=True
        )

        logging.info("Serviço de Rotinas Iniciado...")
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit, InterfaceError):
            logging.info("Serviço finalizado pelo usuário ou erro de interface.")
        finally:
            self.release_lock()

    def check_routines(self):
        logging.info("Verificando rotinas pendentes...")
        try:
            rows = self.consultar(getenv("SQL_ROUTINES_TO_EXECUTE"))['data']

            for row in rows:
                routine = RoutineData.from_row(row)
                Thread(target=self.process_routine, args=(routine,), daemon=True).start()
        except Exception as e:
            logging.error(f"Erro ao buscar rotinas: {e}")
            notify_error(e, "Busca por Rotinas")

    def process_routine(self, routine: RoutineData):
        agora = dt.now()
        dta_agendada = routine.dta_proxima or routine.dta_inicial

        if dta_agendada and dta_agendada <= agora:
            try:
                logging.info(f"Iniciando: {routine.nome} (ID: {routine.id})")

                # Status: Executando
                self.executar(getenv("SQL_UPDATE_SET_TO_E_N"), [routine.id])

                # Dispatcher de tipos
                if routine.tipo == 'RE':
                    self._handle_report(routine)
                elif routine.tipo == 'IN':
                    self._handle_info(routine)
                elif routine.tipo == 'TRG':
                    self._hendle_trigger(routine)

                # Finalização e Reagendamento
                self.executar(getenv("SQL_UPDATE_SET_TO_F_S"), [routine.id])
                self._reschedule(routine, dta_agendada, agora)

                logging.info(f"Sucesso:  {routine.nome} (ID: {routine.id})")

            except Exception as e:
                logging.error(f"Falha na rotina {routine.id} '{routine.nome}': {e}")
                self.executar(getenv("SQL_UPDATE_SET_STATUS_TO_NULL"), [routine.id])
                notify_error(e, routine.nome)

    def _get_hiperlink(self, id_routine: int) -> dict[str, Any]:
       return {
                h[0]: h[1] for h in self.consultar(
                    getenv("SQL_GET_HIPERLINK"),
                    [id_routine]
                )['data']
            }

    def _get_recipient(self, id_routine: int) -> List[str]:
        return [
                    r[0] for r in self.consultar(
                        getenv("SQL_GET_RECIPIENTS"), [id_routine]
                    )['data']
                ]

    def _get_column_names(self, sql: str) -> List[str]:
        """Extrai nomes de colunas dos metadados da consulta."""
        try:
            # Pegamos a descrição do cursor que o seu _database agora retorna
            result = self.consultar(sql)
            if result and 'description' in result:
                return [c[0] for c in result['description']]
            return []
        except Exception as e:
            logging.error(f"Erro ao obter colunas: {e}")
            return []

    def _create_excel(self, colunas, conteudo, nome_rotina) -> Path:
        try:
            wb = Workbook()
            ws = wb.active
            ws.append(colunas)
            for row in conteudo:
                ws.append(row)

            folder = self.base_path / "planilhas"
            folder.mkdir(exist_ok=True)

            # Sanitização de nome de arquivo
            clean_name = normalize('NFD', nome_rotina.lower().replace(' ', '_'))
            clean_name = "".join(c for c in clean_name if category(c) != 'Mn')

            file_path = folder / f"{clean_name}.xlsx"
            wb.save(file_path)
            return file_path
        except Exception as e:
            raise e

    def _reschedule(self, routine: RoutineData, dta_agendada: dt, agora: dt):
        """Calcula e atualiza a próxima execução."""
        try:
            if routine.periodo == 'U' or (routine.dta_final and routine.dta_final <= agora):
                self.executar(getenv("SQL_UPDATE_DISABLE_ROUTINE"), [routine.id])
                return

            # Mapeamento de SQLs de update por período
            sql_map = {
                'Mi': getenv("SQL_UPDATE_SCHEDULE_MINUTE"),
                'H': getenv("SQL_UPDATE_SCHEDULE_HOUR"),
                'D': getenv("SQL_UPDATE_SCHEDULE_DAY"),
                'M': getenv("SQL_UPDATE_SCHEDULE_MONTH")
            }

            sql_update = sql_map.get(routine.periodo)
            if sql_update:
                self.executar(sql_update, [dta_agendada, routine.intervalo, routine.id])
                logging.info(f"Rotina {routine.nome} (ID: {routine.id}) reagendada.")
        except Exception as e:
            raise Exception(f"Erro ao reagendar a rotina: {e}")

    def _handle_info(self, routine: RoutineData):
        try:
            clean_name = normalize('NFD', routine.nome.lower().replace(' ', '_'))
            clean_name = "".join(c for c in clean_name if category(c) != 'Mn')
            # Lógica de informativo (mantida a estrutura de diretórios do original)
            base_info = self.base_path / "informativo"
            base_info.mkdir(exist_ok=True)

            anexos_dir = base_info / "anexos"
            anexos_dir.mkdir(exist_ok=True)
            anexos_dir = base_info / "anexos" / clean_name

            corpos_dir = base_info / "corpos"
            corpos_dir.mkdir(exist_ok=True)
            corpos_dir = base_info / "corpos" / clean_name

            anexos = [str(p) for p in anexos_dir.glob("*")] if anexos_dir.exists() else []
            corpos = [str(p) for p in corpos_dir.glob("*")] if corpos_dir.exists() else []
            destinatarios = self._get_recipient(routine.id)
            hiperlinks = self._get_hiperlink(routine.id)

            posicoes = {nome: i for i, nome in enumerate(hiperlinks.keys())}
            corpos_organizados = sorted(
                corpos,
                key=lambda x: posicoes.get(Path(x).name, len(posicoes))
            )
            Email(
                user=getenv("EMAIL_INFORMATIVO_USER"),
                password=getenv("EMAIL_INFORMATIVO_PASS"),
                cco=destinatarios,
                titulo=f"Informativo - {routine.nome}",
                anexos=anexos,
                corpo_arq=corpos_organizados,
                hyperlink=hiperlinks
            ).enviar()
        except Exception as e:
            raise e

    def _handle_report(self, routine: RoutineData):
        """Lógica de geração e envio de relatório Excel."""
        try:
            # Executa a query principal
            dados = self.consultar(routine.sql)['data']
            # logging.info(f"Dados do db: {dados}")

            # Processamento de datas para exibição no Excel
            dados_formatados = []
            for linha in dados:
                nova_linha = []
                for val in linha:
                    if isinstance(val, dt) and val.strftime("%H:%M:%S") != "00:00:00":
                        nova_linha.append(val.strftime("%d/%m/%Y %H:%M:%S"))
                    elif isinstance(val, dt):
                        nova_linha.append(val.strftime("%d/%m/%Y"))
                    else:
                        nova_linha.append(val)

                dados_formatados.append(nova_linha)

            # Para as colunas, o ideal é que sua classe DB retorne o cursor.description
            # Aqui mantive a lógica de busca por tabela, mas simplificada
            colunas = self._get_column_names(routine.sql)
            path_excel = self._create_excel(colunas, dados_formatados, routine.nome)
            destinatarios = self._get_recipient(routine.id)
            Email(
                para=destinatarios,
                titulo=f"Relatório - {routine.nome}",
                corpo_texto="Segue em anexo o relatório solicitado.",
                anexos=[str(path_excel)]
            ).enviar()
        except Exception as e:
            raise e

    def _hendle_trigger(self, routine: RoutineData):
        logging.info(f"---[ ROTINA TRIGGER '{routine.nome}': ID {routine.id} ]---")





if __name__ == "__main__":
    print("---[ USAR O ARQUIVO MAIN.PY ]---")