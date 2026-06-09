"""Módulo de utilitários e ferramentas de suporte ao sistema."""

import logging
import traceback
from datetime import datetime as dt, timedelta
from logging.handlers import BaseRotatingHandler
from os import getenv
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

from mammoth import convert_to_html

base_path = Path.cwd()

# Tamanho máximo de cada arquivo de log antes de rotacionar (5 MB)
_LOG_MAX_BYTES = 5 * 1024 * 1024

# Quantidade de dias que os arquivos de log são mantidos
_LOG_RETENTION_DAYS = 35


class _MonthlyRotatingFileHandler(BaseRotatingHandler):
    """
    Handler que combina duas políticas de rotação:
      - Por virada de mês: abre um novo arquivo base (service_YYYY-MM.log)
      - Por tamanho: quando o arquivo atual ultrapassa _LOG_MAX_BYTES,
        cria um sufixo incremental (service_YYYY-MM_01.log, _02.log, ...)

    Nomenclatura resultante:
        logs/service_2026-06.log
        logs/service_2026-06_01.log   <- primeiro overflow do mês
        logs/service_2026-06_02.log   <- segundo overflow, etc.
    """

    def __init__(self, log_dir: Path, encoding: str = "utf-8"):
        self.log_dir = log_dir
        self._current_month = dt.now().strftime("%Y-%m")
        filename = self._resolve_path()
        super().__init__(filename, mode="a", encoding=encoding)

    def _resolve_path(self) -> str:
        """
        Retorna o caminho do arquivo de log atual.
        Se o arquivo base do mês já estiver cheio, incrementa o sufixo.
        """
        base = self.log_dir / f"service_{self._current_month}.log"
        if not base.exists() or base.stat().st_size < _LOG_MAX_BYTES:
            return str(base)

        index = 1
        while True:
            candidate = self.log_dir / f"service_{self._current_month}_{index:02d}.log"
            if not candidate.exists() or candidate.stat().st_size < _LOG_MAX_BYTES:
                return str(candidate)
            index += 1

    def shouldRollover(self, record) -> bool:
        month_changed = dt.now().strftime("%Y-%m") != self._current_month
        size_exceeded = self.stream and self.stream.tell() >= _LOG_MAX_BYTES
        return month_changed or size_exceeded

    def doRollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None  # type: ignore[assignment]

        self._current_month = dt.now().strftime("%Y-%m")
        self.baseFilename = self._resolve_path()
        self.stream = self._open()

    def _open(self):
        Path(self.baseFilename).parent.mkdir(parents=True, exist_ok=True)
        return open(self.baseFilename, self.mode, encoding=self.encoding)


def _purge_old_logs(log_dir: Path):
    """Remove arquivos de log mais antigos que _LOG_RETENTION_DAYS dias."""
    limite = dt.now() - timedelta(days=_LOG_RETENTION_DAYS)
    for arq in log_dir.glob("service_*.log"):
        try:
            if dt.fromtimestamp(arq.stat().st_mtime) < limite:
                arq.unlink()
                logging.getLogger("app").info(f"Log antigo removido: {arq.name}")
        except Exception:
            pass


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


def setup_logging(log_dir: Path, level: int = logging.INFO):
    """
    Configura o logging da aplicação com:
      - Rotação automática por tamanho (5 MB) e virada de mês
      - Sub-logger 'app.job' em WARNING — JOBs só aparecem em erro/alteração
      - Bibliotecas de terceiros silenciadas (nível WARNING no root)

    Parâmetros
    ----------
    log_dir : Path
        Diretório onde os arquivos de log serão criados.
    level : int
        Nível de log geral da aplicação (padrão: INFO).
    """
    fmt = logging.Formatter(
        fmt='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Logger raiz em WARNING — silencia bibliotecas de terceiros
    root = logging.getLogger()
    root.setLevel(logging.WARNING)
    root.handlers.clear()

    # Logger principal da aplicação
    app_logger = logging.getLogger("app")
    app_logger.setLevel(level)
    app_logger.propagate = False
    app_logger.handlers.clear()

    file_handler = _MonthlyRotatingFileHandler(log_dir)
    file_handler.setFormatter(fmt)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)

    app_logger.addHandler(file_handler)
    app_logger.addHandler(console_handler)

    # Sub-logger para rotinas do tipo JOB:
    # herda os handlers do pai mas só registra WARNING e acima,
    # suprimindo os INFO de "Iniciando" e "Sucesso" a cada execução.
    job_logger = logging.getLogger("app.job")
    job_logger.setLevel(logging.WARNING)
    job_logger.propagate = True  # usa os handlers do app_logger

    app_logger.info("--- [ Sistema de logs inicializado ] ---")

    # Limpeza de logs antigos a cada inicialização
    _purge_old_logs(log_dir)


def get_logger(tipo: str | None = None) -> logging.Logger:
    """
    Retorna o logger da aplicação.

    Parâmetros
    ----------
    tipo : str | None
        Tipo da rotina. Passe 'JOB' para obter o sub-logger silencioso
        que suprime mensagens INFO de execuções normais.
    """
    if tipo == 'JOB':
        return logging.getLogger("app.job")
    return logging.getLogger("app")


def check_and_update_log_file():
    log_dir = base_path / "logs"
    log_dir.mkdir(exist_ok=True)

    app_logger = get_logger()

    # Reconfigura apenas na primeira execução (sem handlers).
    # A rotação por virada de mês e por tamanho é tratada automaticamente
    # pelo _MonthlyRotatingFileHandler a cada linha escrita.
    if not app_logger.hasHandlers():
        setup_logging(log_dir)


def attempt_error(id_rotina: int) -> tuple[int, bool]:
    """
    Incrementa o contador de tentativas com erro da rotina e retorna o estado atual.

    Retorna
    -------
    (tentativa_atual, limite_atingido)
        tentativa_atual : int  — número da tentativa após o incremento (-1 se indisponível)
        limite_atingido : bool — True se o limite foi atingido e a rotina foi inativada
    """
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