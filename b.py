# from shutil import copy2
# from pathlib import Path
# from tkinter.filedialog import askopenfilename
#
# HOST = '10.3.101.78'
# ANEXO_DIR = f'//{HOST}\\anexos'
# CORPO_DIR = f'//{HOST}\\corpos'
#
#
# def tranferir_arquivos(origem: Path, destino: Path) -> None:
#     # Copia o arquivo
#     try:
#         copy2(origem, destino)
#         print(f"Arquivo copiado com sucesso para {destino}")
#     except Exception as e:
#         print(f"Erro ao copiar: {e}")
#
# def criar_dir(id_rotina: int, anexo: Path, corpo: Path) -> None:
#     if anexo:
#         anexos_rotinas_dir = Path(fr'{ANEXO_DIR}\\rotina_{id_rotina}')
#         anexos_rotinas_dir.mkdir(parents=True, exist_ok=True)
#         print(f"Pasta '{anexos_rotinas_dir}' verificada/criada.")
#         tranferir_arquivos(anexo, anexos_rotinas_dir)
#
#     if corpo:
#         corpos_rotinas_dir = Path(fr'{CORPO_DIR}\\rotina_{id_rotina}')
#         corpos_rotinas_dir.mkdir(parents=True, exist_ok=True)
#         print(f"Pasta '{corpos_rotinas_dir}' verificada/criada.")
#         tranferir_arquivos(corpo, corpos_rotinas_dir)
#
# if __name__ == "__main__":
#     corpos = Path(askopenfilename(title="Corpos"))
#     anexos = Path(askopenfilename(title="Anexos"))
#     criar_dir(id_rotina = 1, anexo = anexos, corpo = corpos)
