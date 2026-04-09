"""Módulo de utilitários e ferramentas de suporte ao sistema."""

import logging
import traceback
from datetime import datetime as dt
from os import getenv
from pathlib import Path
from dataclasses import dataclass
from typing import Optional
from mammoth import convert_to_html

from ._emails import Email
from ._database import DB

base_path = Path.cwd()

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
        # row[8] e row[9] são colunas não utilizadas neste mapeamento
        sql_raw = row[7]
        return cls(
            id=row[0], nome=row[1], periodo=row[2], intervalo=row[3],
            dta_inicial=row[4], dta_proxima=row[5], dta_final=row[6],
            sql=str(sql_raw).upper() if sql_raw else None,
            tipo=row[10]
        )

def setup_logging(log_file):
    """Configura o logging para console e arquivo simultaneamente."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()  # Mantém o log no terminal
        ],
        force=True
    )
    logging.info("--- [ Sistema de logs inicializado ] ---")

def check_and_update_log_file():
    log_dir = base_path / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"service_{dt.now().strftime('%Y-%m')}.log"

    # Lógica: Se o arquivo NÃO existe (virada de mês)
    # OU se o logger ainda não tem handlers (primeira execução do script)
    if not log_file.exists() or not logging.getLogger().hasHandlers():
        logging.info(f"Configurando novo arquivo de log: {log_file.name}")
        setup_logging(log_file)


def attempt_error(id_rotina: int) -> bool|None:
    """
    Verifica a quantidade de tentativas com erro, caso o valor das tentativas seja maior ou igual a 5 a rotina é inativada e a quantidade de tentativas é zerada
    """
    db = DB()
    if db.consultar(getenv("SQL_ATTEMPT_ERROR"),[id_rotina])['data']:
        db.executar(getenv("SQL_UPDATE_RESET_TENT_ERRO"), [id_rotina])
        logging.info(f"---[ Rotina {id_rotina} inativada ]---")

        return True

    db.executar(getenv("SQL_UPDATE_TENT_ERRO"), [id_rotina])

    return None


def notify_error(err: Exception|str, routine: RoutineData|str) -> None:
    """
    Envia um alerta por e-mail com os detalhes técnicos da falha.
    """
    # Se for uma exceção real, pegamos o rastro completo (traceback)
    detalhes_erro = "".join(traceback.format_exception(None, err, err.__traceback__))\
        if isinstance(err,Exception) else str(err)

    if not isinstance(routine, RoutineData):
        nome = routine
        id_rotina = 0
    else:
        nome = routine.nome
        id_rotina = routine.id


    corpo = (
        f"⚠️ ALERTA DE FALHA EM ROTINA\n"
        f"------------------------------------------\n"
        f"Rotina: {nome}\n"
        f"Data/Hora: {dt.now().strftime('%d/%m/%Y %H:%M:%S')}\n"
        f"\nDetalhes Técnicos:\n"
        f"{detalhes_erro}\n"
        f"------------------------------------------\n"
        f"Favor verificar o servidor de automações. "
    )

    if attempt_error(id_rotina):
        corpo += "Rotina inativada, limite de tentativas atingido."
        # print(corpo)

    try:
        Email(
            para=getenv("EMAIL_RECIPIENTS_ERROR").split(","),
            titulo=f"🚨 ERRO CRÍTICO: {nome}",
            corpo_texto=corpo
        ).enviar()
        logging.warning(f"Notificação de erro enviada para a rotina: {nome}")
    except Exception as e:
        logging.critical(f"Falha ao enviar e-mail de notificação de erro: {e}", exc_info=True)

def create_essential_folders():
    log_dir = base_path / "logs"
    log_dir.mkdir(exist_ok=True)

    folder_spreadsheets = base_path / "planilhas"
    folder_spreadsheets.mkdir(exist_ok=True)

    base_info = base_path / "informativo"
    base_info.mkdir(exist_ok=True)

    anexos_dir = base_info / "anexos"
    anexos_dir.mkdir(exist_ok=True)

    corpos_dir = base_info / "corpos"
    corpos_dir.mkdir(exist_ok=True)


def convert_word_to_html(input_path: str) -> str:
    """Converte um arquivo .docx ou .doc para HTML e salva no mesmo diretório."""
    with open(input_path, "rb") as docx_file:
        result = convert_to_html(docx_file)
        html = result.value

        if result.messages:
            for msg in result.messages:
                logging.warning(f"Aviso na conversão Word→HTML: {msg}")

    # Substitui corretamente a extensão independente de ser .docx ou .doc
    p = Path(input_path)
    output_path = str(p.with_suffix(".html"))

    with open(output_path, "w", encoding="utf-8") as html_file:
        html_file.write(html)

    return output_path


if __name__ == "__main__":
    pass