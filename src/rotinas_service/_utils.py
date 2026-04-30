"""Módulo de utilitários e ferramentas de suporte ao sistema."""

import logging
import traceback
from datetime import datetime as dt
from os import getenv
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

from mammoth import convert_to_html

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


def setup_logging(log_file, level: int = logging.INFO):
    """
    Configura o logging exclusivamente para a aplicação.

    Bibliotecas de terceiros (apscheduler, oracledb, mammoth, etc.) são
    silenciadas — apenas erros críticos delas aparecem nos logs.

    Parâmetros
    ----------
    log_file : str | Path
        Caminho do arquivo de log.
    level : int
        Nível de log da aplicação. Exemplos:
          - logging.INFO    -> informações gerais (padrão)
          - logging.WARNING -> apenas avisos e erros
          - logging.ERROR   -> apenas erros
    """
    fmt = logging.Formatter(
        fmt='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Logger raiz em WARNING para silenciar bibliotecas de terceiros
    root = logging.getLogger()
    root.setLevel(logging.WARNING)
    root.handlers.clear()

    # Logger dedicado à aplicação
    app_logger = logging.getLogger("app")
    app_logger.setLevel(level)
    app_logger.propagate = False  # Não repassa ao root — evita duplicidade
    app_logger.handlers.clear()

    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(fmt)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)

    app_logger.addHandler(file_handler)
    app_logger.addHandler(console_handler)

    app_logger.info("--- [ Sistema de logs inicializado ] ---")


def get_logger() -> logging.Logger:
    """Retorna o logger da aplicação. Use em todos os módulos no lugar de logging.*"""
    return logging.getLogger("app")


def check_and_update_log_file():
    log_dir = base_path / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"service_{dt.now().strftime('%Y-%m')}.log"

    app_logger = get_logger()
    if not log_file.exists() or not app_logger.hasHandlers():
        app_logger.info(f"Configurando novo arquivo de log: {log_file.name}")
        setup_logging(log_file)


def attempt_error(id_rotina: int) -> tuple[int, bool]:
    """
    Incrementa o contador de tentativas com erro da rotina e retorna o estado atual.

    Retorna
    -------
    (tentativa_atual, limite_atingido)
        tentativa_atual : int  — número da tentativa após o incremento (-1 se indisponível)
        limite_atingido : bool — True se o limite foi atingido e a rotina foi inativada
    """
    # Import tardio para evitar importação circular (_database importa _utils)
    from ._database import DB

    logger = get_logger()
    db = DB()

    if db.consultar(getenv("SQL_ATTEMPT_ERROR"), [id_rotina])['data']:
        db.executar(getenv("SQL_UPDATE_RESET_TENT_ERRO"), [id_rotina])
        logger.info(f"---[ Rotina {id_rotina} inativada após atingir limite de tentativas ]---")
        return (-1, True)

    db.executar(getenv("SQL_UPDATE_TENT_ERRO"), [id_rotina])

    rows = db.consultar(getenv("SQL_GET_TENT_ERRO"), [id_rotina])['data']
    tentativa_atual = rows[0][0] if rows else -1

    return (tentativa_atual, False)


def notify_error(err: Exception | str, routine: "RoutineData | str") -> None:
    """
    Envia um e-mail de alerta com os detalhes técnicos da falha.
    Deve ser chamado apenas quando todas as tentativas forem esgotadas.
    """
    # Import tardio para evitar importação circular (_emails importa _utils)
    from ._emails import Email

    logger = get_logger()

    detalhes_erro = (
        "".join(traceback.format_exception(None, err, err.__traceback__))
        if isinstance(err, Exception) else str(err)
    )

    nome = routine.nome if isinstance(routine, RoutineData) else routine

    corpo = (
        f"⚠️ ALERTA DE FALHA EM ROTINA\n"
        f"------------------------------------------\n"
        f"Rotina: {nome}\n"
        f"Data/Hora: {dt.now().strftime('%d/%m/%Y %H:%M:%S')}\n"
        f"\nDetalhes Técnicos:\n"
        f"{detalhes_erro}\n"
        f"------------------------------------------\n"
        f"Favor verificar o servidor de automações. "
        f"Rotina inativada, limite de tentativas atingido."
    )

    try:
        Email(
            para=getenv("EMAIL_RECIPIENTS_ERROR").split(";"),
            titulo=f"🚨 ERRO CRÍTICO: {nome}",
            corpo_texto=corpo
        ).enviar()
        logger.warning(f"Notificação de erro enviada para a rotina: {nome}")
    except Exception as e:
        logger.critical(f"Falha ao enviar e-mail de notificação de erro: {e}", exc_info=True)


def create_essential_folders():
    log_dir = base_path / "logs"
    log_dir.mkdir(exist_ok=True)

    (base_path / "planilhas").mkdir(exist_ok=True)

    base_info = base_path / "informativo"
    base_info.mkdir(exist_ok=True)
    (base_info / "anexos").mkdir(exist_ok=True)
    (base_info / "corpos").mkdir(exist_ok=True)


def convert_word_to_html(input_path: str) -> str:
    """Converte um arquivo .docx ou .doc para HTML e salva no mesmo diretório."""
    logger = get_logger()
    with open(input_path, "rb") as docx_file:
        result = convert_to_html(docx_file)
        html = result.value

        if result.messages:
            for msg in result.messages:
                logger.warning(f"Aviso na conversão Word->HTML: {msg}")

    output_path = str(Path(input_path).with_suffix(".html"))

    with open(output_path, "w", encoding="utf-8") as html_file:
        html_file.write(html)

    return output_path


if __name__ == "__main__":
    pass
