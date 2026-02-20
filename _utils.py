"""Módulo de utilitários e ferramentas de suporte ao sistema."""

import logging
import traceback
from datetime import datetime as dt
from os import getenv
from pathlib import Path
from _emails import Email

base_path = Path.cwd()

def setup_logging():
    """Configura o logging para console e arquivo simultaneamente."""
    log_dir = base_path / "logs"
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / f"service_{dt.now().strftime('%Y-%m')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()  # Mantém o log no terminal
        ]
    )
    logging.info("--- [ Sistema de logs inicializado ] ---")


def notify_error(err: str|Exception, routine_name: str) -> None:
    """
    Envia um alerta por e-mail com os detalhes técnicos da falha.
    """
    # Se for uma exceção real, pegamos o rastro completo (traceback)
    detalhes_erro = "".join(traceback.format_exception(None, err, err.__traceback__))\
        if isinstance(err,Exception) else str(err)

    corpo = (
        f"⚠️ ALERTA DE FALHA EM ROTINA\n"
        f"------------------------------------------\n"
        f"Rotina: {routine_name}\n"
        f"Data/Hora: {dt.now().strftime('%d/%m/%Y %H:%M:%S')}\n"
        f"\nDetalhes Técnicos:\n"
        f"{detalhes_erro}\n"
        f"------------------------------------------\n"
        f"Favor verificar o servidor de automações."
    )

    try:
        Email(
            para=getenv("EMAIL_RECIPIENTS_ERROR"),
            titulo=f"🚨 ERRO CRÍTICO: {routine_name}",
            corpo_texto=corpo
        ).enviar()
        logging.warning(f"Notificação de erro enviada para a rotina: {routine_name}")
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


if __name__ == "__main__":
    setup_logging()
    notify_error("Erro de teste", 'Teste de falha na rotina')