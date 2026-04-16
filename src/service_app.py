import sys
from dotenv import load_dotenv

# Carrega variáveis de ambiente antes de qualquer import do pacote
load_dotenv()

from rotinas_service import RoutineService, check_and_update_log_file, create_essential_folders, get_logger

create_essential_folders()
check_and_update_log_file()


def start_service():
    logger = get_logger()
    logger.info("--- [ Iniciando Sistema de Gestão de Rotinas ] ---")
    try:
        rotinas = RoutineService()
        rotinas.run()
    except KeyboardInterrupt:
        logger.info("Serviço interrompido manualmente (Ctrl+C).")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Erro fatal no loop principal: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    start_service()