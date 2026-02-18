"""Módulo de utilitários e ferramentas"""

import logging
from _emails import Email


def active_config_logging():
    """Configuração de Logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )



def notify_error(err:str|Exception, routine_name: str) -> None:
    """Envio de notificação via e-mail em caso de falha em alguma rotina.
    :rtype: None
    """
    active_config_logging()
    Email(
        para=["pedrorodrigues@grupomonaco.com.br"],
        titulo=f"[ ATENÇÃO ] Falha na rotina - {routine_name}",
        corpo_texto=f"Falha na rotina - {routine_name}: \n\nErro: {err}"
    ).enviar()

if __name__ == "__main__":
    notify_error("Erro de teste", 'Teste de falha na rotina')