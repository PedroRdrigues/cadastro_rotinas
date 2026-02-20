import logging
import sys
from dotenv import load_dotenv
from _utils import setup_logging
from _rotinas import RoutineService

# Configura o log antes de qualquer outra coisa
setup_logging()

# Carrega as variáveis de ambiente
load_dotenv(verbose=True)

def start_service():
    logging.info("--- [ Iniciando Sistema de Gestão de Rotinas ] ---")

    try:
        # Instancia o serviço
        rotinas = RoutineService()
        rotinas.run()

    except KeyboardInterrupt:
        logging.info("Serviço interrompido manualmente (Ctrl+C).")
        sys.exit(0)

    except Exception as e:
        # Aqui o log vai tanto para o terminal quanto para o arquivo .log
        logging.critical(f"Erro fatal no loop principal: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    start_service()