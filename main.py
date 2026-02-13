"""Ponto de entrada principal para o Serviço de Rotinas Automáticas."""

import logging
from sys import exit
from _rotinas import RoutineService

# Reutilizamos a configuração de log que já está nos outros módulos
# Mas garantimos que erros críticos aqui no main também sejam registrados
logger = logging.getLogger(__name__)


def start_service():
    """Inicializa e executa o serviço de rotinas."""
    logging.info("--- [ Iniciando Sistema de Gestão de Rotinas ] ---")

    try:
        # Instancia o serviço (isso já valida o Pool do DB e o Lock de arquivo)
        rotinas = RoutineService()

        # Inicia o Scheduler (isso trava a execução aqui até o serviço ser parado)
        rotinas.run()

    except KeyboardInterrupt:
        logging.info("Serviço interrompido manualmente pelo usuário (Ctrl+C).")
        exit(0)

    except Exception as e:
        logging.critical(f"Erro fatal ao iniciar o arquivo principal: {e}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    start_service()