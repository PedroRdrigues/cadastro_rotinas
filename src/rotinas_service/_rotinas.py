from ._database import DB, InterfaceError
from ._emails import Email
from ._utils import (
    notify_error, attempt_error, check_and_update_log_file,
    RoutineData, convert_word_to_html, get_logger
)


from datetime import datetime as dt
from pathlib import Path
from typing import List, Any
from unicodedata import category, normalize
from atexit import register
from msvcrt import locking, LK_NBLCK
from os import getpid, _exit, getenv
from time import sleep

# Dependências externas
try:
    from openpyxl import Workbook
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.interval import IntervalTrigger
except ImportError as e:
    get_logger().error(f"Dependência faltando: {e}")


# Intervalo (segundos) em que o serviço verifica novas rotinas ou alterações no banco
_SYNC_INTERVAL = 5


class RoutineService:

    # Número máximo de tentativas por execução antes de contabilizar erro acumulado.
    # Deve ser igual ao limite configurado na query SQL_ATTEMPT_ERROR.
    _MAX_RETRIES = 5

    # Tempo base de espera (segundos) entre tentativas — cresce linearmente.
    # Tentativa 1→2: 10s | 2→3: 20s | 3→4: 30s | 4→5: 40s
    _RETRY_BACKOFF = 10

    def __init__(self):
        super().__init__()
        self.db = DB()
        self.base_path = Path.cwd()
        self.lock_file_path = self.base_path / "service.lock"
        self.lock_handle = None
        self.scheduler: BlockingScheduler | None = None

        # Mantém o estado dos jobs registrados: {id_rotina: (intervalo_segundos, dta_proxima)}
        # Usado pelo sync para detectar mudanças de intervalo OU de dta_proxima no banco.
        self._registered: dict[int, tuple[int | None, dt | None]] = {}

        register(self.release_lock)

    # ------------------------------------------------------------------
    # Lock de instância única
    # ------------------------------------------------------------------

    def release_lock(self):
        if self.lock_handle:
            try:
                self.lock_handle.close()
                get_logger().info("--- [ Trava de arquivo liberada ] ---")
            except Exception:
                pass

    def acquire_lock(self):
        """Garante que apenas uma instância do serviço rode por vez."""
        logger = get_logger()
        for _ in range(5):
            try:
                self.lock_handle = open(self.lock_file_path, "w")
                locking(self.lock_handle.fileno(), LK_NBLCK, 1)
                self.lock_handle.write(str(getpid()))
                self.lock_handle.flush()
                logger.info(f"Lock adquirido (PID: {getpid()})")
                return True
            except (OSError, IOError):
                logger.warning("Outra instância em execução. Tentando novamente...")
                sleep(1)

        logger.error("Não foi possível adquirir o lock. Encerrando.")
        _exit(0)

    # ------------------------------------------------------------------
    # Inicialização e loop principal
    # ------------------------------------------------------------------

    def run(self):
        logger = get_logger()
        self.acquire_lock()

        self.scheduler = BlockingScheduler(timezone="America/Sao_Paulo")

        # Job de sincronização: detecta novas rotinas ou alterações no banco
        self.scheduler.add_job(
            self._sync_jobs,
            IntervalTrigger(seconds=_SYNC_INTERVAL),
            id="__sync__",
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )

        # Carga inicial: registra todas as rotinas ativas antes de iniciar o loop
        self._sync_jobs()

        logger.info("Serviço de Rotinas Iniciado...")
        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Serviço finalizado pelo usuário ou erro de interface.")
        finally:
            self.release_lock()

    # ------------------------------------------------------------------
    # Sincronização de jobs
    # ------------------------------------------------------------------

    def _sync_jobs(self):
        """
        Consulta o banco e mantém os jobs do scheduler sincronizados com as
        rotinas ativas. Adiciona rotinas novas, remove as inativadas e
        recarrega as que tiveram o intervalo alterado — sem reiniciar o serviço.
        """
        logger = get_logger()
        check_and_update_log_file()

        try:
            rows = self.db.consultar(getenv("SQL_ROUTINES_TO_EXECUTE"))['data']
        except Exception as e:
            logger.error(f"Erro ao sincronizar rotinas: {e}")
            notify_error(e, "Sincronização de Rotinas")
            return

        rotinas_ativas: dict[int, RoutineData] = {}
        for row in rows:
            try:
                r = RoutineData.from_row(row)
                rotinas_ativas[r.id] = r
            except Exception as e:
                logger.error(f"Erro ao mapear linha de rotina: {e} | row={row}")

        ids_banco = set(rotinas_ativas.keys())
        ids_registrados = set(self._registered.keys())

        # Remove jobs de rotinas que foram inativadas ou removidas no banco
        for rid in ids_registrados - ids_banco:
            self._remove_job(rid)

        # Adiciona ou atualiza jobs
        for rid, routine in rotinas_ativas.items():
            intervalo_atual = self._intervalo_segundos(routine)
            dta_atual = routine.dta_proxima or routine.dta_inicial

            if rid not in self._registered:
                self._add_job(routine)

            else:
                intervalo_reg, dta_reg = self._registered[rid]

                if intervalo_reg != intervalo_atual:
                    # Intervalo mudou — recria o job com trigger completamente novo
                    logger.info(f"Intervalo alterado para rotina '{routine.nome}' (ID: {rid}). Reagendando.")
                    self._remove_job(rid)
                    self._add_job(routine)

                elif dta_reg != dta_atual:
                    # Apenas dta_proxima foi alterada externamente no banco —
                    # atualiza somente o start_date do trigger sem recriar o job
                    self._reschedule_trigger(routine)
                    self._registered[rid] = (intervalo_atual, dta_atual)

    def _intervalo_segundos(self, routine: RoutineData) -> int | None:
        """Converte período + intervalo da rotina para segundos (usado como chave de comparação)."""
        if routine.periodo == 'U':
            return None
        mult = {'S': 1, 'Mi': 60, 'H': 3600, 'D': 86400, 'M': 2592000}
        return mult.get(routine.periodo, 60) * (routine.intervalo or 1)

    def _make_trigger(self, routine: RoutineData) -> IntervalTrigger:
        """Cria um IntervalTrigger com base no período e intervalo da rotina."""
        kwargs = {
            'S':  {'seconds':  routine.intervalo},
            'Mi': {'minutes':  routine.intervalo},
            'H':  {'hours':    routine.intervalo},
            'D':  {'days':     routine.intervalo},
            'M':  {'weeks':    routine.intervalo},
        }
        params = kwargs.get(routine.periodo, {'minutes': routine.intervalo or 1})

        # start_date garante que o job só dispara a partir da data agendada
        start = routine.dta_proxima or routine.dta_inicial
        return IntervalTrigger(**params, start_date=start, timezone="America/Sao_Paulo")

    def _job_id(self, routine_id: int) -> str:
        return f"rotina_{routine_id}"

    def _add_job(self, routine: RoutineData):
        """Registra um job no scheduler para a rotina."""
        logger = get_logger()
        try:
            trigger = self._make_trigger(routine)
            self.scheduler.add_job(
                self.process_routine,
                trigger=trigger,
                args=[routine],
                id=self._job_id(routine.id),
                name=routine.nome,
                # max_instances=1 por job: se a execução anterior ainda estiver
                # rodando quando o próximo tick chegar, o novo disparo é ignorado
                # para aquela rotina específica — sem afetar nenhuma outra.
                max_instances=1,
                coalesce=True,
                replace_existing=True,
            )
            self._registered[routine.id] = (
                self._intervalo_segundos(routine),
                routine.dta_proxima or routine.dta_inicial,
            )
            logger.info(f"Job registrado: '{routine.nome}' (ID: {routine.id}) | intervalo: {routine.intervalo}{routine.periodo}")
        except Exception as e:
            logger.error(f"Erro ao registrar job da rotina {routine.id}: {e}")

    def _reschedule_trigger(self, routine: RoutineData):
        """
        Atualiza apenas o start_date do trigger do job existente quando
        dta_proxima foi alterada externamente no banco.
        O job não é removido nem recriado — só o próximo disparo é movido.
        """
        logger = get_logger()
        try:
            nova_data = routine.dta_proxima or routine.dta_inicial
            novo_trigger = self._make_trigger(routine)
            self.scheduler.reschedule_job(
                self._job_id(routine.id),
                trigger=novo_trigger,
            )
            logger.info(
                f"dta_proxima alterada externamente: '{routine.nome}' (ID: {routine.id}) "
                f"— próxima execução: {nova_data}"
            )
        except Exception as e:
            logger.error(f"Erro ao atualizar trigger da rotina {routine.id}: {e}")

    def _remove_job(self, routine_id: int):
        """Remove um job do scheduler e do registro interno."""
        logger = get_logger()
        try:
            self.scheduler.remove_job(self._job_id(routine_id))
            logger.info(f"Job removido: rotina ID {routine_id}")
        except Exception:
            raise Exception(f'Erro ao remover a rotina {routine_id}')
        self._registered.pop(routine_id, None)

    # ------------------------------------------------------------------
    # Execução de rotina com retry
    # ------------------------------------------------------------------

    def process_routine(self, routine: RoutineData):
        """
        Ponto de entrada de cada job. Executa a rotina com até _MAX_RETRIES
        tentativas em caso de falha, com backoff linear entre elas.
        Ao esgotar as tentativas, incrementa o contador acumulado no banco e,
        se o limite for atingido, inativa a rotina e envia notificação por e-mail.
        """
        logger = get_logger()
        ultimo_erro: Exception | None = None

        for tentativa in range(1, self._MAX_RETRIES + 1):
            try:
                if tentativa > 1:
                    espera = (tentativa - 1) * self._RETRY_BACKOFF
                    logger.warning(
                        f"Tentativa {tentativa}/{self._MAX_RETRIES} para "
                        f"'{routine.nome}' (ID: {routine.id}) — aguardando {espera}s..."
                    )
                    sleep(espera)

                logger.info(f"Iniciando: '{routine.nome}' (ID: {routine.id})")

                self.db.executar(getenv("SQL_UPDATE_SET_TO_EXECUTE"), [routine.id])

                if routine.tipo == 'RE':
                    self._handle_report(routine)
                elif routine.tipo == 'IN':
                    self._handle_info(routine)
                elif routine.tipo == 'TRG':
                    self._handle_trigger(routine)
                elif routine.tipo == 'JOB':
                    self._handle_job(routine)

                self.db.executar(getenv("SQL_UPDATE_SET_TO_FINISHED"), [routine.id])

                # Atualiza dta_proxima no banco para todos os tipos exceto JOB
                if routine.tipo != 'JOB':
                    self._reschedule(routine)

                # Recarrega o objeto da rotina no job com os dados mais recentes
                self._refresh_job(routine)

                logger.info(f"Sucesso: '{routine.nome}' (ID: {routine.id})")
                return  # Saída limpa — não executa o bloco de erro abaixo

            except Exception as e:
                ultimo_erro = e
                logger.error(
                    f"Falha [{tentativa}/{self._MAX_RETRIES}] na rotina "
                    f"'{routine.nome}' (ID: {routine.id}): {e}"
                )
                self.db.executar(getenv("SQL_UPDATE_SET_STATUS_TO_NULL"), [routine.id])

        # Todas as tentativas falharam — incrementa contador acumulado no banco
        # e notifica apenas se o limite total de erros for atingido
        _tentativa_banco, limite_atingido = attempt_error(routine.id)
        if limite_atingido:
            notify_error(ultimo_erro, routine)

    def _reschedule(self, routine: RoutineData):
        """
        Calcula e grava a próxima data de execução no banco.
        Chamado após toda execução bem-sucedida, exceto para rotinas do tipo JOB.
        Para período 'U' ou quando dta_final é atingida, desativa a rotina.
        """
        logger = get_logger()
        agora = dt.now()

        try:
            if routine.periodo == 'U' or (routine.dta_final and routine.dta_final <= agora):
                self.db.executar(getenv("SQL_UPDATE_DISABLE_ROUTINE"), [routine.id])
                self._remove_job(routine.id)
                logger.info(f"Rotina '{routine.nome}' (ID: {routine.id}) inativada após execução única/final.")
                return

            sql_map = {
                'S':  getenv("SQL_UPDATE_SCHEDULE_SECOND"),
                'Mi': getenv("SQL_UPDATE_SCHEDULE_MINUTE"),
                'H':  getenv("SQL_UPDATE_SCHEDULE_HOUR"),
                'D':  getenv("SQL_UPDATE_SCHEDULE_DAY"),
                'M':  getenv("SQL_UPDATE_SCHEDULE_MONTH"),
            }

            sql_update = sql_map.get(routine.periodo)
            if sql_update:
                base = agora.replace(second=0, microsecond=0)
                self.db.executar(sql_update, [base, routine.intervalo, routine.id])
                logger.info(f"dta_proxima atualizada: '{routine.nome}' (ID: {routine.id})")
            else:
                logger.warning(
                    f"Período '{routine.periodo}' sem SQL de reagendamento definido "
                    f"(rotina ID: {routine.id})."
                )
        except Exception as e:
            raise Exception(f"Erro ao reagendar rotina '{routine.nome}': {e}") from e

    def _refresh_job(self, routine: RoutineData):
        """
        Recarrega os dados da rotina do banco e atualiza o argumento do job,
        garantindo que dta_proxima e outros campos reflitam o estado atual
        sem precisar remover e recriar o job. Também sincroniza _registered
        para que o próximo sync não reaja à mudança que o próprio serviço fez.
        """
        try:
            rows = self.db.consultar(getenv("SQL_GET_ROUTINE_BY_ID"), [routine.id])['data']
            if rows:
                routine_atualizada = RoutineData.from_row(rows[0])
                job = self.scheduler.get_job(self._job_id(routine.id))
                if job:
                    job.modify(args=[routine_atualizada])
                    self._registered[routine.id] = (
                        self._intervalo_segundos(routine_atualizada),
                        routine_atualizada.dta_proxima or routine_atualizada.dta_inicial,
                    )
        except Exception as e:
            get_logger().warning(f"Não foi possível atualizar o job da rotina {routine.id}: {e}")

    # ------------------------------------------------------------------
    # Helpers de banco
    # ------------------------------------------------------------------

    def _get_hiperlink(self, id_routine: int) -> dict[str, Any]:
        return {
            h[0]: h[1] for h in self.db.consultar(
                getenv("SQL_GET_HIPERLINK"), [id_routine]
            )['data']
        }

    def _get_recipient(self, id_routine: int) -> List[str]:
        return [
            r[0] for r in self.db.consultar(
                getenv("SQL_GET_RECIPIENTS"), [id_routine]
            )['data']
        ]

    # ------------------------------------------------------------------
    # Handlers de tipo de rotina
    # ------------------------------------------------------------------

    def _get_corpos(self, routine_id):
        base_info = self.base_path / "informativo"
        nome = f"rotina_{routine_id}"

        corpos_dir = base_info / "corpos" / nome
        corpos_dir.mkdir(parents=True, exist_ok=True)

        corpos = [str(p) for p in corpos_dir.glob("*")] if corpos_dir.exists() else []
        hiperlinks = self._get_hiperlink(routine_id)

        arq_banco = set(hiperlinks.keys())
        corpos = [a for a in corpos if Path(a).name in arq_banco]

        arq_word = next((a for a in corpos if a.endswith(".docx") or a.endswith(".doc")), None)
        if arq_word:
            return [convert_word_to_html(arq_word)], hiperlinks

        posicoes = {nome_arq: i for i, nome_arq in enumerate(hiperlinks.keys())}
        corpos_organizados = sorted(
            corpos,
            key=lambda x: posicoes.get(Path(x).name, len(posicoes))
        )
        return corpos_organizados, hiperlinks

    def _create_excel(self, result: dict, routine: RoutineData) -> Path:
        colunas = [c[0] for c in result['description']] if result.get('description') else []

        dados_formatados = []
        for linha in result['data']:
            nova_linha = []
            for val in linha:
                if isinstance(val, dt) and val.strftime("%H:%M:%S") != "00:00:00":
                    nova_linha.append(val.strftime("%d/%m/%Y %H:%M:%S"))
                elif isinstance(val, dt):
                    nova_linha.append(val.strftime("%d/%m/%Y"))
                else:
                    nova_linha.append(val)
            dados_formatados.append(nova_linha)

        wb = Workbook()
        ws = wb.active
        ws.append(colunas)
        for row in dados_formatados:
            ws.append(row)

        folder = self.base_path / "planilhas"
        folder.mkdir(exist_ok=True)

        clean_name = normalize('NFD', routine.nome.lower().replace(' ', '_'))
        clean_name = "".join(c for c in clean_name if category(c) != 'Mn')

        file_path = folder / f"{clean_name}xlsx"
        wb.save(file_path)
        return file_path

    def _handle_info(self, routine: RoutineData):
        self.db.executar(getenv("SQL_DELETE_INATIVE_EMAILS"))

        nome = f"rotina_{routine.id}"
        base_info = self.base_path / "informativo"
        base_info.mkdir(exist_ok=True)

        anexos_dir = base_info / "anexos" / nome
        anexos_dir.mkdir(parents=True, exist_ok=True)

        destinatarios = self._get_recipient(routine.id)
        anexos = [str(p) for p in anexos_dir.glob("*")]
        corpos, hiperlinks = self._get_corpos(routine.id)

        try:
            Email(
                user=getenv("EMAIL_INFORMATIVO_USER"),
                password=getenv("EMAIL_INFORMATIVO_PASS"),
                cco=destinatarios,
                titulo=f"Informativo - {routine.nome}",
                anexos=anexos,
                corpo_arq=corpos,
                hyperlink=hiperlinks
            ).enviar()
        except Exception as e:
            raise Exception(f"Erro ao processar informativo '{routine.nome}': {e}") from e

    def _handle_report(self, routine: RoutineData):
        """Geração e envio de relatório Excel."""
        try:
            result = self.db.consultar(routine.sql)
            path_excel = self._create_excel(result, routine)
            destinatarios = self._get_recipient(routine.id)
            Email(
                para=destinatarios,
                titulo=f"Relatório - {routine.nome}",
                corpo_texto="Segue em anexo o relatório solicitado."
                            "\n\nEste é um e-mail automático, favor não responder."
                            "\n\nAtenciosamente, Departamento de TI.",
                anexos=[str(path_excel)]
            ).enviar()
        except Exception as e:
            raise Exception(f"Erro ao processar relatório '{routine.nome}': {e}") from e

    def _handle_trigger(self, routine: RoutineData):
        """
        Executa a query da rotina e, se retornar dados, gera e envia o relatório.
        Útil para relatórios condicionais: só envia quando há registros relevantes.
        Reutiliza o result já consultado para não fazer duas chamadas ao banco.
        """
        try:
            result = self.db.consultar(routine.sql)
            if not result['data']:
                return

            path_excel = self._create_excel(result, routine)
            destinatarios = self._get_recipient(routine.id)
            Email(
                para=destinatarios,
                titulo=f"Relatório - {routine.nome}",
                corpo_texto="Segue em anexo o relatório solicitado."
                            "\n\nEste é um e-mail automático, favor não responder."
                            "\n\nAtenciosamente, Departamento de TI.",
                anexos=[str(path_excel)]
            ).enviar()
        except Exception as e:
            raise Exception(f"Erro ao processar trigger '{routine.nome}': {e}") from e

    def _handle_job(self, routine: RoutineData):
        try:
            self.db.executar(routine.sql)
        except Exception as e:
            raise Exception(f"Erro ao executar JOB '{routine.nome}': {e}") from e


if __name__ == "__main__":
    print("---[ USAR O ARQUIVO SERVICE_APP.PY ]---")