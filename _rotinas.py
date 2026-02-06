"""
    SERVIÇO DE CONSULTA E EXECUÇÃO DO CADASTRO DE ROTINAS\n
    Criar um serviço que faça consultas a cada minuto numa tabela no banco de dados.\n
    O seriviço deve executar o Script SQL que está informado na coluna SQL na tabela, baseado na ESCALA(HORA, DIA, SEMANA, MÊS, ANO) e no INTERVALO a partir da DTA_INICIAL.\n
    Também deverá atualizar a coluna DTA_PROXIMA com a data(dia/mês/ano hh:mm:ss).
"""

from _databases import Oracle
from _emails import Email

from atexit import register
from msvcrt import locking, LK_NBLCK
from time import sleep
from datetime import datetime as dt
from threading import Thread
from os import getcwd, path, mkdir, listdir, getpid, _exit, getenv

try:
    from openpyxl import Workbook
except ImportError as e:
    print("Openpyxl não instalado")
try:
    from apscheduler.schedulers.blocking import BlockingScheduler
except ImportError as e:
    print("Apscheduler não instalado")

# Environmental variables SQLs
SQL_ROUTINES_TO_EXECUTE = getenv("SQL_ROUTINES_TO_EXECUTE")
SQL_CHECK_EMAIL_SENT = getenv("SQL_CHECK_EMAIL_SENT")
SQL_UPDATE_SET_TO_E = getenv("SQL_UPDATE_SET_TO_E")
SQL_UPDATE_SCHEDULE_MINUTE = getenv("SQL_UPDATE_SCHEDULE_MINUTE")
SQL_UPDATE_SCHEDULE_HOUR=getenv("SQL_UPDATE_SCHEDULE_HOUR")
SQL_UPDATE_SCHEDULE_DAY=getenv("SQL_UPDATE_SCHEDULE_DAY")
SQL_UPDATE_SCHEDULE_MHONTH=getenv("SQL_UPDATE_SCHEDULE_MHONTH")
SQL_UPDATE_DISABLE_ROUTINE=getenv("SQL_DISABLE_ROUTINE")
SQL_UPDATE_SET_TO_F = getenv("SQL_UPDATE_SET_TO_F")
SQL_UPDATE_SET_TO_NULL = getenv("SQL_UPDATE_SET_TO_NULL")
SQL_UPDATE_EMAIL_SENT_TO_N=getenv("SQL_UPDATE_EMAIL_SENT_TO_N")
SQL_GET_COLUMN_NAMES=getenv("SQL_GET_COLUMN_NAMES")
SQL_GET_RECIPIENTS=getenv("SQL_GET_RECIPIENTS")
SQL_UPDATE_EMAIL_SENT_TO_S=getenv("SQL_UPDATE_EMAIL_SENT_TO_S")


class Rotinas(Oracle):
    """
    Manipula o processo de verificação de agendamentos das _rotinas cadastradas criando uma nova Thread para cada rotina.\n
    As "voltas" de verificação das _rotinas são feitas a cada 5 min sempre no segundo .00.\n
    Quando uma rotina cadastrada tem a data e horário de agendamento igual a data e horário atual ele cria a Thread para Executá-la.\n
    Mesmo que uma rotina tenha sua data e horário de execução agendado para um horário entre uma "volta" e outra, ela será executada normamente.\n
    As _rotinas são divididas em dois tipos "RELATORIOS" e "INFORMATIVOS"
    """
    def __init__(self):
        # Inicializa o DB (cria o pool)
        super().__init__()

        # Caminho para o arquivo de trava (Lock)
        self.lock_file_path = fr"{getcwd()}\servico.lock"
        self.lock_file = None

        # Registra a função para fechar a trava sempre que o script encerrar
        register(self.release_lock)

    def release_lock(self):
        """Libera o handle do arquivo para o Windows soltar a trava."""
        if self.lock_file:
            try:
                self.lock_file.close()
                print(f"\n--- [{dt.now()}] Trava de arquivo liberada. ---")
            except:
                pass

    def acquire_lock(self) -> bool:
        """Tenta adquirir a trava com retentativas para suportar reinícios rápidos."""
        for _ in range(5):
            try:
                self.lock_file = open(self.lock_file_path, "w")

                # Tenta travar o arquivo usando o LK_NBLCK
                locking(self.lock_file.fileno(), LK_NBLCK, 1)

                # Grava o PID atual no arquivo para conferência
                self.lock_file.write(str(getpid()))
                self.lock_file.flush()
                print(f"\n---[{dt.now()}] Trava adquirida com sucesso (PID: {getpid()})---")

                return True

            except (OSError, IOError):
                print(f"[{dt.now()}] ERRO: Outra instância já está em execução. Encerrando...")

                if self.lock_file:
                    self.lock_file.close()
                sleep(1)

        _exit(0)

    def run(self) -> None:
        """Inicia o processo de verificação de agendamentos das _rotinas cadastradas com 1 segundo entre cada execução."""
        # Passo 1: Bloqueia qualquer execução duplicada
        self.acquire_lock()

        # Passo 2: Configura o Agendador Avançado
        # Usamos o BlockingScheduler porque o serviço deve ficar "preso" aqui
        scheduler = BlockingScheduler()

        # Adiciona o Job com proteções extras:
        # - misfire_grace_time=10: Se o serviço reiniciar e atrasar, ele tem 10s de tolerância.
        # - coalesce=True: Se o PC travar e voltar, ele não dispara 10 vezes os jobs acumulados.
        scheduler.add_job(
            self.verifica_rotinas,
            'cron',
            second='0',
            id='envio_rotinas',
            misfire_grace_time=10,
            coalesce=True
        )

        try:
            scheduler.start()
            sleep(1)
        except (KeyboardInterrupt, SystemExit):
            print("\n---[ Serviço finalizado ]---")
        finally:
            if self.lock_file:
                self.lock_file.close()


    def verifica_rotinas(self) -> None:
        """
        Realiza a consulta no banco de dados e cria uma Thread para cada execução de rotina ativa.\n
        As "voltas" de verificação das _rotinas são feitas a cada 5 min sempre no segundo .00.
        """
        # ATENÇÃO: A data atual deve ser pega AGORA, não no __init__
        agora = dt.now()
        print(f"\n---[ Verificando _rotinas: {agora} ]---")

        try:
            rows = self.consultar(SQL_ROUTINES_TO_EXECUTE)
        except Exception as e:
            print(f"Erro ao buscar _rotinas: {e}")
            raise Exception(f"Erro ao buscar _rotinas: {e}")

        for row in rows:

            # Ajuste os índices conforme a sua tabela real no Oracle
            # Inicia a Thread passando os dados da linha
            # self.threadName = f"Thread_{i}"
            t = Thread(target=self.executaRotina, args=[row])
            t.start()


    def executaRotina(self, cadastroRotina: list) -> None:
        """
        Recebe o cadastro de uma rotina e faz a extração e tratamento dos dados.\n
        Verifica a data de agendamento e compara com a data atual para realizar a execução somente das _rotinas necessárias.\n
        Verifica o "Tipo de Rotina", chama o metodo responsavel pela execução específica de cada tipo: "RELATORIO" ou "INFORMATIVO".\n
        Após a execução, verifica ser a coluna "ENVIADO" no banco de dados foi atualizada para "SIM" e atualiza a data do próximo agendamento com base na coluna "ESCALA", cado necessário.\n
        Caso o valor da coluna "ESCALA" seja "UNITARIO" é realizada a inativação da rotina.
        """
        # Desempacotando (Garanta que a ordem das colunas no SELECT * bate com isso aqui)
        id_rotina = cadastroRotina[0]
        nome = cadastroRotina[1]
        periodo = cadastroRotina[2]
        intervalo = cadastroRotina[3]
        dta_inicial = cadastroRotina[4]  # Oracle já retorna datetime objects
        dta_proxima = cadastroRotina[5]
        dta_final = cadastroRotina[6]
        sql_consulta = cadastroRotina[7] if cadastroRotina[7] else None
        tipo = cadastroRotina[10]

        # Lógica de Data: Se dta_proxima for None, usa a inicial.
        dta_agendada = dta_proxima if dta_proxima else dta_inicial

        agora = dt.now()

        # Verifica se já está na hora de rodar (ou se está atrasado)
        if dta_agendada and dta_agendada <= agora:
            # print(f"Executando: {self.threadName}")
            # print(f"Executando: {nome}")
            try:
                # Atualiza a coluna status para E
                self.executar(
                    SQL_UPDATE_SET_TO_E,
                    [id_rotina]
                )
                # Execuções de relatórios.
                if tipo == 'RE':
                    # print(f"Executando: {self.threadName} - Rolatório")
                    self.relatorio(
                        sql=sql_consulta,
                        nome_rotina=nome,
                        id_rotina=id_rotina
                    )

                elif tipo == 'IN':
                    # print(f"Executando: {self.threadName} - Informativo")
                    self.informativo(
                        nome_rotina=nome,
                        id_rotina=id_rotina
                    )

                # Atualiza a coluna status para F
                self.executar(
                    SQL_UPDATE_SET_TO_F,
                    [id_rotina]
                )

                if periodo != 'U' or dta_final == agora:
                    # Verifica se o e-mail foi enviado.
                    enviado = self.consultar(
                        SQL_CHECK_EMAIL_SENT,
                        [id_rotina]
                    )
                    if enviado[0][0] == "S":
                        # Atualiza a próxima data
                        sql_update = ""

                        if periodo == 'MI':
                            sql_update = SQL_UPDATE_SCHEDULE_MINUTE

                        elif periodo == 'H':
                            sql_update = SQL_UPDATE_SCHEDULE_HOUR

                        elif periodo == 'D':
                            sql_update = SQL_UPDATE_SCHEDULE_DAY

                        elif periodo == 'M':
                            sql_update = SQL_UPDATE_SCHEDULE_MHONTH

                        if sql_update:
                            # Geralmente usa-se a agendada para não encavalar horários, mas para simplificar usei a agendada.
                            self.executar(sql_update, [dta_agendada, intervalo, id_rotina])
                            print(f"\n-- [ Próxima data atualizada ] ---\n")
                else:
                    self.executar(
                        SQL_UPDATE_DISABLE_ROUTINE,
                        [id_rotina]
                    )

            except Exception as e:
                print(f"ERRO na execução do executaRotina(): {e}")
                self.executar(
                    SQL_UPDATE_SET_TO_NULL,
                    [id_rotina]
                )
                raise Exception(f"ERRO na execução do executaRotina(): {e}")


    def relatorio(self, sql:str, nome_rotina, id_rotina):
        """Execução das _rotinas do tipo: "Relatorio"."""
        print(sql, type(sql))
        sql = str(sql).upper()
        print(sql, type(sql))
        try:
           # 1. Atualiza a coluna 'ENVIADO' para 'N'
            self.executar(
                SQL_UPDATE_EMAIL_SENT_TO_N,
                [id_rotina]
            )
            # 2. Executa verifica o nome das colunas da tabela/view dentro da query cadastrada na rotina
            nome_tabela = sql.split('FROM')[1].strip().split(" ")[0] \
                if len(sql.split('FROM')[1].strip().split(" ")[0].split('.')) == 1 \
                else sql.split('FROM')[1].strip().split(" ")[0].split('.')[1]



            list_colunas = self.consultar(
                SQL_GET_COLUMN_NAMES,
                [nome_tabela]
            )
            colunas = []
            for coluna in list_colunas:
                colunas.append(coluna[0])

            # 2. Executa a query da rotina
            retorno = self.consultar(sql)

            # 2.1 Verificar se existe algum valor de data e converter para o formato de data padão
            for index, value in enumerate(retorno):
                valorCelula = [i for i in value]

                for i, v in enumerate(valorCelula):
                    if isinstance(v, dt):
                        retorno[index][i] = v.strftime("%d/%m/%Y %H:%M:%S")

            # 3. Usa o retorno e cria uma tabela em formato Excel(.xlsx)
            caminho_arquivo = self.criaExcel(
                colunas=colunas,
                conteundo=retorno,
                nome_rotina=nome_rotina,
                )

            # 4. Consulta a lista de destinatarios para onde devem ser enviados os relatórios
            destinatarios = self.consultar(
                SQL_GET_RECIPIENTS,
                [id_rotina]
            )
            destinatarios = [i[0] for i in destinatarios]

            # 5. Faz o envio do relatório
            email = Email(
                para=destinatarios,
                titulo=nome_rotina,
                # corpo_texto=f"Olá,\n\nSegue em anexo o {nome_rotina}.",
                anexos=[caminho_arquivo]
            )

            email.enviar()
            # 6. Atualiza a coluna 'enviado' do cadastro da rotina para 'SIM'
            self.executar(
                SQL_UPDATE_EMAIL_SENT_TO_S,
                [id_rotina]
            )

        except Exception as e:
            print(f"ERRO na execução do relatorio(): {e}")
            raise Exception(f"ERRO na execução do relatorio(): {e}")


    def criaExcel(self, colunas, conteundo, nome_rotina):
        """Cria uma planilha Excel e armazena dentro do diretório "Planilhas"."""
        # Cria um arquivo .xlsx vazio
        workbook = Workbook()
        # Cria a página ativa do arquivo
        sheet = workbook.active
        # Adiciona o nome das colunas na página ativa
        sheet.append(colunas)

        # Adiciona o conteúdo na página ativa
        for row in conteundo:
            sheet.append(row)

        if not path.exists(f'{getcwd()}/planilhas'):
            mkdir(f'{getcwd()}/planilhas')

        caminho_arquivo = fr'{getcwd()}/planilhas/{nome_rotina}.xlsx'

        workbook.save(caminho_arquivo)
        sleep(1)

        return caminho_arquivo


    def informativo(self, nome_rotina,  id_rotina):
        """Execução das _rotinas do tipo: "Informativo"."""
        EMAIL_INFORMATIVO_USER = getenv("EMAIL_INFORMATIVO_USER")
        EMAIL_INFORMATIVO_PASS = getenv("EMAIL_INFORMATIVO_PASS")

        try:
           # 1. Atualiza a coluna 'ENVIADO' para 'N'
            self.executar(
                SQL_UPDATE_EMAIL_SENT_TO_N,
                [id_rotina]
            )
            caminho_anexos = []
            caminho_corpo = []

            if path.exists(f"{getcwd()}/informativo/anexos/{nome_rotina}"):
                caminho_anexo = f"{getcwd()}/informativo/anexos/{nome_rotina}"
                anexos = listdir(caminho_anexo)
                caminho_anexos = [f"{caminho_anexo}/{a}" for a in anexos]

            if path.exists(f"{getcwd()}/informativo/corpos/{nome_rotina}"):
                caminho_corpo = f"{getcwd()}/informativo/corpos/{nome_rotina}"
                imgs_corpo = listdir(caminho_corpo)
                caminho_corpo = [f"{caminho_corpo}/{img}" for img in imgs_corpo]

            # 2. Consulta a lista de destinatarios para onde devem ser enviados os relatórios
            emails = self.consultar(
                SQL_GET_RECIPIENTS,
                [id_rotina]
            )
            emails = [i[0] for i in emails]

            email = Email(
                user=EMAIL_INFORMATIVO_USER,
                password=EMAIL_INFORMATIVO_PASS,
                cco=emails,
                titulo=f"Informativo - {nome_rotina}",
                anexos=caminho_anexos,
                corpo_arq=caminho_corpo
            )

            email.enviar()
            sleep(3)

            # 4. Atualiza a coluna 'enviado' do cadastro da rotina para 'S'
            self.executar(
                SQL_UPDATE_EMAIL_SENT_TO_S,
                [id_rotina]
            )

        except Exception as e:
            print(f"ERRO na execução do informativo(): {e}")
            raise Exception(f"ERRO na execução do informativo(): {e}")


if __name__ == "__main__":
     print("Use o arquivo main.py!")
     r = Rotinas()
     t = r.consultar(SQL_ROUTINES_TO_EXECUTE)
     print(t)
     for i in t:
         print(i)

