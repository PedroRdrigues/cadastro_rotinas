from os import getenv
from shutil import copy2, rmtree
from pathlib import Path
from tkinter.filedialog import askopenfilename
from typing import Optional

from src.rotinas_service import DB, RoutineData

HOST = '10.3.101.78'
ANEXO_DIR = f'//{HOST}\\anexos'
CORPO_DIR = f'//{HOST}\\corpos'

DB_USER_PROD= getenv("DB_USER_PROD")
DB_PASS_PROD= getenv("DB_PASS_PROD")
DB_DSN_PROD= getenv("DB_DSN_PROD")

db = DB()#user=DB_USER_PROD, password=DB_PASS_PROD, dsn=DB_DSN_PROD)


# adicionar cadastro
def cadastrar_rotina(nome: str, periodo: str, intervalo: int|None, dta_inicial: str, dta_final: str|None,
        consulta: str|None, tipo:str, ativo:str = "S") -> list:

    db.executar(
        "BEGIN p_pro_cadastrar_rotinas(:1, :2, :3, to_date(:4, 'DD/MM/YYYY'), :5, :6, :7, :8); END;",
        [nome, periodo, intervalo, dta_inicial, dta_final, consulta, tipo, ativo]
    )
    row = db.consultar(
        "SELECT * FROM cadastro_rotinas ORDER BY id_rotina DESC FETCH FIRST 1 ROWS ONLY"
    )["data"][0]

    print(row)
    return row

def adicionar_emails(id_rotina: int, emails: list|str) -> list:
    if isinstance(emails, str):
        emails = emails.split(";")

    for email in emails:
        db.executar("INSERT INTO email_rotinas VALUES(:1, :2, pedro_dev.seq_email_rotinas.nextval)", [id_rotina, email])


    emails = db.consultar("SELECT id_email, email FROM email_rotinas WHERE id_rotina = :1", [id_rotina])["data"]
    print(emails)
    for email in emails:

        print('|', email[0], "|", email[1])
    print('\n', len(emails),'emails adicionados.')

    return emails

def tranferir_arquivos(origem: Path|str, destino: Path|str) -> None:
    # Copia o arquivo
    try:
        copy2(origem, destino)
        print(f"Arquivo copiado com sucesso para {destino}")
    except Exception as e:
        print(f"Erro ao copiar: {e}")

def criar_dir(id_rotina: int, anexo: Optional[str|Path] = None, corpo: Optional[str|Path] = None) -> Path:
    if anexo:
        anexos_rotinas_dir = Path(fr'{ANEXO_DIR}\\rotina_{id_rotina}')
        anexos_rotinas_dir.mkdir(parents=True, exist_ok=True)
        print(f"Pasta '{anexos_rotinas_dir}' verificada/criada.")
        tranferir_arquivos(anexo, anexos_rotinas_dir)
        return anexos_rotinas_dir

    if corpo:
        corpos_rotinas_dir = Path(fr'{CORPO_DIR}\\rotina_{id_rotina}')
        corpos_rotinas_dir.mkdir(parents=True, exist_ok=True)
        print(f"Pasta '{corpos_rotinas_dir}' verificada/criada.")
        tranferir_arquivos(corpo, corpos_rotinas_dir)
        return corpos_rotinas_dir

def apaga_dir(path: Path) -> None:
    if path.exists() and path.is_dir():
        rmtree(path)
    else:
        print("Diretório inexistente.")

def adicionar_arquivos(id_rotina: int) -> None:
    quant_corpos = int(input("Quantos corpos deseja adicionar?[0 se nenhum] "))
    for i in range(quant_corpos):
        corpos = Path(askopenfilename(title="Corpos"))
        hiperlink = input("Qual o hiperlink? ")
        if hiperlink == '':
            hiperlink = None
        print(hiperlink)
        remote_dir = criar_dir(id_rotina, corpo=corpos)
        try:
            db.executar("INSERT INTO estrutura_informativo VALUES(seq_estrurura_informativo.nextval, :1, :2, :3, :4, NULL)",[id_rotina,hiperlink, corpos.name, (i+1)])
            print("Corpo adicionado com sucesso.")
        except Exception as e:
            apaga_dir(remote_dir)
            raise Exception(f"Erro ao adicionar corpo: {e}")

    quant_anexos = int(input("Quantos anexos deseja adicionar?[0 se nenhum] "))
    for i in range (quant_anexos):
        anexos = Path(askopenfilename(title="Anexos"))
        remote_dir = criar_dir(id_rotina, anexo=anexos)
        try:
            db.executar("INSERT INTO estrutura_informativo VALUES(seq_estrurura_informativo.nextval, :1, NULL, NULL, NULL, :2)",[id_rotina,anexos.name])
            print("Anexo adicionado com sucesso.")
        except Exception as e:
            apaga_dir(remote_dir)
            raise Exception(f"Erro ao adicionar anexo: {e}")

if __name__ == "__main__":
    try:
        routine = RoutineData.from_row(
            cadastrar_rotina(
                nome="teste rotina",
                periodo="u",
                intervalo=None,
                dta_inicial="22/03/2026",
                dta_final=None,
                consulta=None,
                tipo="IN",
                ativo="S"
            )
        )

        try:
            emails="pedrorodrigues@grupomonaco.com.br;jeanvidigal@grupomonaco.com.br;joelsonsantos@grupomonaco.com.br"
            email = adicionar_emails(id_rotina=routine.id, emails=emails)

        except Exception as e:
            db.executar("DELETE FROM cadastro_rotinas where id_rotina = :1", [routine.id])
            raise Exception(f"Erro ao adicionar os e-mail: {e}")

        try:
            if routine.tipo == "IN":
                adicionar_arquivos(routine.id)

        except Exception as e:
            db.executar("DELETE FROM email_rotinas WHERE id_rotina = :1", [routine.id])
            db.executar("DELETE FROM cadastro_rotinas where id_rotina = :1", [routine.id])

            raise Exception(f"erro na adição dos arquivos: {e}")
    except Exception as e:
        print(e)


