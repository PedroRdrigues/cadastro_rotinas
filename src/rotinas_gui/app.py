import customtkinter as ctk
from pathlib import Path
from tkinter import Menu
from tkinter.messagebox import showerror, showinfo, showwarning
from tkinter.filedialog import askopenfilename
from datetime import datetime as dt

from conn import tranferir_arquivos, DB

# Configurações globais do CustomTkinter
# ctk.set_appearance_mode("light")  # "dark" ou "light"
ctk.set_default_color_theme("blue")  # Temas: "blue", "green", "dark-blue"


def parse_hora(entry_value: str) -> dt.time:
    """
    Converte uma string no formato HH, HH:MM ou HH:MM:SS para um objeto time.
    Retorna meia-noite (00:00:00) em caso de valor inválido.
    """
    partes = entry_value.strip().split(':')
    partes = (partes + ['00', '00', '00'])[:3]
    try:
        hora_str = f"{int(partes[0]):02}:{int(partes[1]):02}:{int(partes[2]):02}"
        return dt.strptime(hora_str, '%H:%M:%S').time()
    except (ValueError, IndexError):
        return dt.strptime("00:00:00", '%H:%M:%S').time()


def msg_error(err):
    showerror(title="Erro", message=str(err))


def msg_warning(warning):
    showwarning(title="Aviso", message=str(warning))


class App(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("Rotinas")
        self.geometry("650x500")
        self.minsize(650, 500)
        self.resizable(False, True)

        self.mapa_tipos = {
            "Relatório": "RE",
            "Informativo": "IN"
        }
        self.mapa_periodos = {
            "Único": 'U',
            "Minuto": 'Mi',
            "Hora": 'H',
            "Dia": 'D',
            "Mês": 'M'
        }

        self._reset_estado()
        self._criar_menu_bar()
        self.layout()
        self.db = DB()

    # ------------------------------------------------------------------
    # Menu Bar
    # ------------------------------------------------------------------

    def _criar_menu_bar(self):
        """Cria a barra de menus da aplicação."""
        self.menu_bar = Menu(self)
        self.config(menu=self.menu_bar)

        menu_opcoes = Menu(self.menu_bar, tearoff=0)
        self.menu_bar.add_cascade(label="Opções", menu=menu_opcoes)
        menu_opcoes.add_command(label="Buscar rotina", command=self._abrir_busca)

    # ------------------------------------------------------------------
    # Estado interno
    # ------------------------------------------------------------------

    def _reset_estado(self):
        """Reinicia todas as variáveis de estado sem recriar widgets."""
        self.arq_sql = None
        self.dict_anexo = {}
        self.dict_corpo = {}
        self.dict_links = {}
        self.data_inicial_completa = None
        self.data_final_completa = None

        # StringVars reutilizáveis entre chamadas de layout
        self.var_nome = ctk.StringVar()
        self.var_ativo = ctk.StringVar(value="S")
        self.var_tipos_sel = ctk.StringVar(value="Selecione")
        self.var_periodos_sel = ctk.StringVar(value="Selecione")

    @property
    def var_tipos(self) -> str:
        """Retorna o código interno do tipo selecionado (ex: 'RE', 'IN')."""
        return self.mapa_tipos.get(self.var_tipos_sel.get(), '')

    @property
    def var_periodos(self) -> str:
        """Retorna o código interno do período selecionado (ex: 'U', 'D')."""
        return self.mapa_periodos.get(self.var_periodos_sel.get(), '')

    # ------------------------------------------------------------------
    # Cadastro
    # ------------------------------------------------------------------

    def cadastrar_rotina(self):
        try:
            self._validar_campos()

            self.data_inicial_completa = dt.combine(
                dt.strptime(self.entry_data_inicial.get(), "%d/%m/%Y").date(),
                parse_hora(self.entry_hora_inicial.get())
            )

            if self.entry_data_final.get().strip():
                self.data_final_completa = dt.combine(
                    dt.strptime(self.entry_data_final.get(), "%d/%m/%Y").date(),
                    parse_hora(self.entry_hora_final.get())
                )
            else:
                self.data_final_completa = None

            destinatarios = (
                self.entry_destinatarios.get("1.0", "end-1c")
                .replace(';', ' ')
                .split()
            )

            consulta = None
            if self.arq_sql:
                with open(self.arq_sql, "r") as f:
                    consulta = f.read()

            intervalo = int(self.entry_intervalo.get()) if self.entry_intervalo.get().strip() else 0

            id_rotina = self.db.inserir_cadastro_rotina(
                nome=self.var_nome.get().title(),
                periodo=self.var_periodos,
                intervalo=intervalo,
                dta_inicial=self.data_inicial_completa,
                dta_final=self.data_final_completa,
                consulta=consulta,
                tipo=self.var_tipos,
                ativo=self.var_ativo.get(),
                destinatarios=destinatarios,
                corpos=self.dict_corpo if self.dict_corpo else None,
                hiperlinks=self.dict_links if self.dict_links else None,
                anexos=self.dict_anexo if self.dict_anexo else None,
            )

            if self.var_tipos == "IN":
                if self.dict_corpo:
                    self._inserir_arquivos_server(id_rotina, 'corpo', self.dict_corpo)
                if self.dict_anexo:
                    self._inserir_arquivos_server(id_rotina, 'anexo', self.dict_anexo)

            showinfo(
                title='Rotinas',
                message=f'Cadastro da rotina "{self.var_nome.get()}" (ID: {id_rotina}) realizado com sucesso.'
            )
            self.limpa_tudo()

        except ValueError as e:
            msg_error(f"Formato de data inválido. Use DD/MM/AAAA.\nDetalhe: {e}")
        except Exception as e:
            msg_error(f"Erro ao realizar o cadastro: {e}")

    def _validar_campos(self):
        """Lança Exception com mensagem amigável se algum campo obrigatório estiver inválido."""
        if self.var_tipos not in self.mapa_tipos.values():
            raise Exception("Tipo de rotina não informado.")

        if not self.var_nome.get().strip():
            raise Exception("Favor informar o nome da rotina.")

        if self.var_periodos not in self.mapa_periodos.values():
            raise Exception("Favor selecionar um Período.")

        if not self.entry_intervalo.get().strip() and self.var_periodos != 'U':
            raise Exception("Favor informar o Intervalo.")

        if self.entry_intervalo.get().strip():
            try:
                int(self.entry_intervalo.get())
            except ValueError:
                raise Exception("O intervalo deve ser um número inteiro.")

        if not self.entry_data_inicial.get().strip():
            raise Exception("Favor informar a data de início.")

        if not self.entry_destinatarios.get("1.0", "end-1c").strip():
            raise Exception("Favor informar o(s) destinatário(s).")

        if not self.arq_sql and self.var_tipos == "RE":
            raise Exception("Insira o arquivo de consulta SQL.")

        if self.var_tipos == 'IN' and (not self.dict_anexo and not self.dict_corpo):
            raise Exception("Favor inserir pelo menos um anexo ou corpo para o e-mail.")

        count_doc = sum(
            1 for v in self.dict_corpo.values()
            if str(v).lower().endswith(('.docx', '.doc'))
        )
        if count_doc > 1:
            raise Exception("O Corpo deve conter apenas um arquivo .docx ou .doc.")

    # ------------------------------------------------------------------
    # Utilitários
    # ------------------------------------------------------------------

    def _inserir_arquivos_server(self, id_rotina: int, tipo: str, target_dict: dict):
        for _, origem in target_dict.items():
            tranferir_arquivos(
                id_rotina=id_rotina,
                origem=Path(origem),
                tipo=tipo,
            )

    def limpa_tudo(self):
        """Destrói e recria todo o layout, reiniciando o estado."""
        for widget in self.winfo_children():
            widget.destroy()
        self._reset_estado()
        self.layout()

    def get_consulta_sql(self):
        file_path = askopenfilename(filetypes=[("SQL", "*.sql")])
        if file_path:
            self.arq_sql = file_path
            name = Path(file_path).name
            display_name = f" {name[:15]}...{Path(file_path).suffix} " if len(name) > 20 else f" {name} "
            self.label_arq_consulta.configure(text=display_name, text_color="#2ecc71")
            self.btn_limpa_arq_consulta.grid(row=0, column=3, padx=10, sticky="e")

    def limpa_consulta_sql(self):
        self.arq_sql = None
        self.label_arq_consulta.configure(text=' "seu_arquivo.sql" ', text_color="gray")
        self.btn_limpa_arq_consulta.grid_forget()

    def select_frame_from_type(self, choice):
        tipo = self.mapa_tipos.get(choice, '')
        if tipo == "RE":
            self._cria_frame_consulta()
        elif tipo == "IN":
            self._cria_frame_informativo()

    def select_data_final_from_type(self, choice):
        periodo = self.mapa_periodos.get(choice, '')
        if periodo == 'U':
            self.frame_data_final.grid_forget()
        else:
            self.frame_data_final.grid(row=1, column=0, columnspan=4, pady=5)

    def _cria_frame_consulta(self):
        self.dict_anexo.clear()
        self.dict_corpo.clear()
        if self.frame_informativos.winfo_ismapped():
            self.frame_informativos.pack_forget()
        self.frame_consulta.pack(fill="both", expand=True, padx=10, pady=5)
        self.frame_cadastrar.pack(fill="x", side="bottom")

    def _cria_frame_informativo(self):
        self.limpa_consulta_sql()
        if self.frame_consulta.winfo_ismapped():
            self.frame_consulta.pack_forget()
        self.frame_cadastrar.pack_forget()

        if not self.frame_informativos.winfo_exists():
            self.func_frame_informativos()

        self.frame_informativos.pack(fill="both", expand=True, padx=10, pady=5)
        self.frame_cadastrar.pack(fill="x", side="bottom")

    def adiciona_anexo(self, target_dict: dict, target_frame):
        try:
            file_path = askopenfilename()
            if not file_path:
                return

            if target_dict is self.dict_corpo:
                extensoes_validas = ('.docx', '.doc', '.jpeg', '.png', '.jpg')
                if Path(file_path).suffix.lower() not in extensoes_validas:
                    raise Exception(
                        "O corpo deve conter apenas imagens (.jpeg, .png, .jpg) "
                        "ou documentos Word (.docx, .doc)."
                    )

            novo_id = max(target_dict.keys()) + 1 if target_dict else 1
            target_dict[novo_id] = file_path
            self.atualiza_label_anexos(target_dict, target_frame)

        except Exception as e:
            msg_warning(e)

    def exclue_anexo(self, key: int, target_dict: dict, target_frame):
        target_dict.pop(key, None)
        self.dict_links.pop(key, None)
        self.atualiza_label_anexos(target_dict, target_frame)

    def mover_corpo(self, key: int, direcao: int):
        """
        Move o item de chave `key` no dict_corpo uma posição para cima (direcao=-1)
        ou para baixo (direcao=+1), mantendo dict_links sincronizado.
        As chaves são renumeradas sequencialmente após o reordenamento.
        """
        chaves = list(self.dict_corpo.keys())
        idx = chaves.index(key)
        novo_idx = idx + direcao

        if novo_idx < 0 or novo_idx >= len(chaves):
            return  # já está no limite

        # Troca posições na lista de chaves
        chaves[idx], chaves[novo_idx] = chaves[novo_idx], chaves[idx]

        # Reconstrói dict_corpo e dict_links com chaves sequenciais (1, 2, 3...)
        novo_corpo = {}
        novo_links = {}
        for nova_chave, velha_chave in enumerate(chaves, start=1):
            novo_corpo[nova_chave] = self.dict_corpo[velha_chave]
            if velha_chave in self.dict_links:
                novo_links[nova_chave] = self.dict_links[velha_chave]

        self.dict_corpo.clear()
        self.dict_corpo.update(novo_corpo)
        self.dict_links.clear()
        self.dict_links.update(novo_links)

        self.atualiza_label_anexos(self.dict_corpo, self.frame_corpo_lista)

    def atualiza_label_anexos(self, target_dict: dict, target_frame):
        for widget in target_frame.winfo_children():
            widget.destroy()

        chaves = list(target_dict.keys())
        is_corpo = target_dict is self.dict_corpo

        for k, v in target_dict.items():
            suffix = Path(v).suffix.lower()
            name = Path(v).name
            short_name = f"{name[:15]}~{suffix}" if len(name) > 20 else name

            f = ctk.CTkFrame(target_frame, fg_color="transparent")
            f.pack(fill="x", pady=2)

            # Botões de reordenação (somente no corpo)
            if is_corpo:
                idx = chaves.index(k)

                btn_up = ctk.CTkButton(
                    f, text="▲", width=22, height=20,
                    fg_color="#3498db", hover_color="#2980b9",
                    state="normal" if idx > 0 else "disabled",
                    command=lambda k=k: self.mover_corpo(k, -1)
                )
                btn_up.pack(side="left", padx=(5, 1))

                btn_down = ctk.CTkButton(
                    f, text="▼", width=22, height=20,
                    fg_color="#3498db", hover_color="#2980b9",
                    state="normal" if idx < len(chaves) - 1 else "disabled",
                    command=lambda k=k: self.mover_corpo(k, +1)
                )
                btn_down.pack(side="left", padx=(1, 5))

            ctk.CTkLabel(f, text=short_name, font=("Helvetica", 11)).pack(side="left", padx=5)

            ctk.CTkButton(
                f, text="X", width=20, height=20,
                fg_color="#e74c3c", hover_color="#c0392b",
                command=lambda k=k, d=target_dict, fr=target_frame: self.exclue_anexo(k, d, fr)
            ).pack(side="right", padx=5)

            is_image = suffix in ('.jpeg', '.png', '.jpg')

            if is_corpo and is_image:
                self.create_hiperlink_entry(f, k)

    def create_hiperlink_entry(self, master_frame, key):
        valor_anterior = self.dict_links.get(key, "")

        entry_link = ctk.CTkEntry(
            master_frame,
            placeholder_text="Insira o hiperlink da imagem...",
            height=22,
            font=("Helvetica", 10),
        )

        if valor_anterior:
            entry_link.insert(0, valor_anterior)

        entry_link.pack(side="left", fill="x", expand=True, padx=10)
        entry_link.bind("<KeyRelease>", lambda event, e=entry_link: self.salva_texto_link(key, e.get()))

    def salva_texto_link(self, key, texto: str):
        self.dict_links[key] = texto

    # ------------------------------------------------------------------
    # Layout
    # ------------------------------------------------------------------

    def layout(self):
        self._criar_menu_bar()
        self.main_container = ctk.CTkScrollableFrame(self, corner_radius=10)
        self.main_container.pack(fill="both", expand=True, padx=15, pady=15)

        self.func_frame_tipo_ativo()
        self.func_frame_nome()
        self.func_frame_periodo_intervalo()
        self.func_frame_datas()
        self.func_frame_destinatarios()
        self.func_frame_informativos()   # criado mas oculto até seleção de tipo
        self.func_frame_consulta()       # criado mas oculto até seleção de tipo
        self.func_frame_cadastrar()

    def func_frame_tipo_ativo(self):
        frame = ctk.CTkFrame(self.main_container, fg_color="transparent")
        frame.pack(fill="x", padx=10, pady=5)
        frame.columnconfigure(2, weight=1)

        ctk.CTkLabel(frame, text="Tipo", font=("Helvetica", 12, "bold")).grid(row=0, column=0, padx=10)
        self.box_tipos = ctk.CTkOptionMenu(
            frame,
            variable=self.var_tipos_sel,
            values=list(self.mapa_tipos.keys()),
            command=self.select_frame_from_type
        )
        self.box_tipos.grid(row=0, column=1, padx=10)

        ctk.CTkLabel(frame, text="Ativo", font=("Helvetica", 12, "bold")).grid(row=0, column=3, padx=10)
        ctk.CTkRadioButton(frame, text="Sim", variable=self.var_ativo, value="S").grid(row=0, column=4, padx=5)
        ctk.CTkRadioButton(frame, text="Não", variable=self.var_ativo, value="N").grid(row=0, column=5, padx=5)

    def func_frame_nome(self):
        frame = ctk.CTkFrame(self.main_container, fg_color="transparent")
        frame.pack(fill="x", padx=10, pady=5)

        ctk.CTkLabel(frame, text="Nome", font=("Helvetica", 12, "bold")).grid(row=0, column=0, padx=10, sticky="w")
        self.entry_nome = ctk.CTkEntry(
            frame, textvariable=self.var_nome, width=498,
            placeholder_text="Digite o nome da rotina..."
        )
        self.entry_nome.grid(row=0, column=1, padx=10)

    def func_frame_periodo_intervalo(self):
        frame = ctk.CTkFrame(self.main_container, fg_color="transparent")
        frame.pack(fill="x", padx=10, pady=5)
        frame.columnconfigure(2, weight=1)

        ctk.CTkLabel(frame, text="Período", font=("Helvetica", 12, "bold")).grid(row=0, column=0, padx=10)
        self.box_periodos = ctk.CTkOptionMenu(
            frame,
            variable=self.var_periodos_sel,
            values=list(self.mapa_periodos.keys()),
            command=self.select_data_final_from_type
        )
        self.box_periodos.grid(row=0, column=1, padx=10)

        ctk.CTkLabel(frame, text="Intervalo", font=("Helvetica", 12, "bold")).grid(row=0, column=3, padx=10)
        self.entry_intervalo = ctk.CTkEntry(frame, width=100, placeholder_text="Ex: 1")
        self.entry_intervalo.grid(row=0, column=4, padx=10)

    def func_frame_datas(self):
        self.frame_datas_master = ctk.CTkFrame(self.main_container, fg_color="transparent")
        self.frame_datas_master.pack(fill="x", padx=10, pady=5)

        ctk.CTkLabel(self.frame_datas_master, text="Início", font=("Helvetica", 12, "bold")).grid(
            row=0, column=0, padx=10)
        self.entry_data_inicial = ctk.CTkEntry(self.frame_datas_master, placeholder_text="DD/MM/AAAA")
        self.entry_data_inicial.grid(row=0, column=1, padx=5)

        self.entry_hora_inicial = ctk.CTkEntry(self.frame_datas_master, placeholder_text="HH:MM:SS", width=100)
        self.entry_hora_inicial.grid(row=0, column=2, padx=5)

        self.frame_data_final = ctk.CTkFrame(self.frame_datas_master, fg_color="transparent")

        ctk.CTkLabel(self.frame_data_final, text="Fim   ", font=("Helvetica", 12, "bold")).grid(
            row=0, column=0, padx=10)
        self.entry_data_final = ctk.CTkEntry(self.frame_data_final, placeholder_text="DD/MM/AAAA")
        self.entry_data_final.grid(row=0, column=1, padx=5)

        self.entry_hora_final = ctk.CTkEntry(self.frame_data_final, placeholder_text="HH:MM:SS", width=100)
        self.entry_hora_final.grid(row=0, column=2, padx=5)

    def func_frame_destinatarios(self):
        frame = ctk.CTkFrame(self.main_container, fg_color="transparent")
        frame.pack(fill="x", padx=10, pady=5)

        ctk.CTkLabel(frame, text="Destinatários", font=("Helvetica", 12, "bold")).pack(anchor="nw", padx=10)
        self.entry_destinatarios = ctk.CTkTextbox(frame, height=80, width=580)
        self.entry_destinatarios.pack(padx=10, pady=5)

    def func_frame_consulta(self):
        self.frame_consulta = ctk.CTkFrame(self.main_container)

        ctk.CTkLabel(self.frame_consulta, text="Consulta SQL", font=("Helvetica", 12, "bold")).grid(
            row=0, column=0, padx=10, pady=10)

        ctk.CTkButton(
            self.frame_consulta, text="+", fg_color="#2ecc71",
            command=self.get_consulta_sql, width=30
        ).grid(row=0, column=1, padx=10)

        self.label_arq_consulta = ctk.CTkLabel(
            self.frame_consulta, text=' "seu_arquivo.sql" ', text_color="gray")
        self.label_arq_consulta.grid(row=0, column=2, padx=10)

        self.btn_limpa_arq_consulta = ctk.CTkButton(
            self.frame_consulta, text="X", width=30, fg_color="#e74c3c",
            command=self.limpa_consulta_sql
        )

    def func_frame_informativos(self):
        self.frame_informativos = ctk.CTkFrame(self.main_container)

        # Barra de Anexos
        top_bar_anexos = ctk.CTkFrame(self.frame_informativos, fg_color="transparent")
        top_bar_anexos.pack(fill="x", padx=5, pady=5)

        ctk.CTkLabel(top_bar_anexos, text="Anexos", font=("Helvetica", 12, "bold")).pack(side="left", padx=10)
        ctk.CTkButton(
            top_bar_anexos, text="+", width=30, fg_color="#2ecc71", hover_color="#27ae60",
            command=lambda: self.adiciona_anexo(self.dict_anexo, self.frame_anexos_lista)
        ).pack(side="left")

        self.frame_anexos_lista = ctk.CTkFrame(self.frame_informativos, height=30)
        self.frame_anexos_lista.pack(fill="x", padx=10, pady=5)

        # Barra de Corpo
        top_bar_corpo = ctk.CTkFrame(self.frame_informativos, fg_color="transparent")
        top_bar_corpo.pack(fill="x", padx=5, pady=5)

        ctk.CTkLabel(top_bar_corpo, text="Corpo", font=("Helvetica", 12, "bold")).pack(side="left", padx=10)
        ctk.CTkButton(
            top_bar_corpo, text="+", width=30, fg_color="#2ecc71", hover_color="#27ae60",
            command=lambda: self.adiciona_anexo(self.dict_corpo, self.frame_corpo_lista)
        ).pack(side="left")

        self.frame_corpo_lista = ctk.CTkFrame(self.frame_informativos, height=30)
        self.frame_corpo_lista.pack(fill="x", padx=10, pady=5)

    def func_frame_cadastrar(self):
        self.frame_cadastrar = ctk.CTkFrame(self.main_container, fg_color="transparent")
        self.frame_cadastrar.pack(fill="x", side="bottom", pady=20)

        ctk.CTkButton(
            self.frame_cadastrar, text="CADASTRAR ROTINA",
            command=self.cadastrar_rotina, font=("Helvetica", 14, "bold"), height=45
        ).pack(fill="x", padx=50)


    def _abrir_busca(self):
        JanelaBusca(self, self.db)


# ===========================================================================
# Janela de Busca
# ===========================================================================

class JanelaBusca(ctk.CTkToplevel):
    """
    Janela modal para busca de rotinas por nome e/ou ID.
    Exibe os resultados em uma tabela e abre a JanelaEdicao ao clicar em uma linha.
    """

    COLUNAS = ("ID", "Nome", "Tipo", "Período", "Ativo")
    COL_WIDTHS = (50, 280, 90, 80, 50)

    def __init__(self, master: App, db: DB):
        super().__init__(master)
        self.db = db
        self.master_app = master

        self.title("Buscar Rotina")
        self.geometry("600x420")
        self.minsize(600, 420)
        self.resizable(True, True)
        self.grab_set()  # modal
        self.focus_set()

        self._build_ui()

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------

    def _build_ui(self):
        # ── Filtros ────────────────────────────────────────────────────
        frame_filtros = ctk.CTkFrame(self, fg_color="transparent")
        frame_filtros.pack(fill="x", padx=15, pady=(15, 5))

        ctk.CTkLabel(frame_filtros, text="Nome:", font=("Helvetica", 12, "bold")).grid(
            row=0, column=0, padx=(0, 5), sticky="w")
        self.entry_nome = ctk.CTkEntry(frame_filtros, width=280, placeholder_text="Parte do nome...")
        self.entry_nome.grid(row=0, column=1, padx=(0, 15))
        self.entry_nome.bind("<Return>", lambda e: self._buscar())

        ctk.CTkLabel(frame_filtros, text="ID:", font=("Helvetica", 12, "bold")).grid(
            row=0, column=2, padx=(0, 5), sticky="w")
        self.entry_id = ctk.CTkEntry(frame_filtros, width=80, placeholder_text="Ex: 42")
        self.entry_id.grid(row=0, column=3, padx=(0, 15))
        self.entry_id.bind("<Return>", lambda e: self._buscar())

        ctk.CTkButton(
            frame_filtros, text="Buscar", width=80,
            command=self._buscar
        ).grid(row=0, column=4)

        # ── Tabela (canvas + scrollbar nativa) ─────────────────────────
        import tkinter as tk

        frame_tabela = ctk.CTkFrame(self)
        frame_tabela.pack(fill="both", expand=True, padx=15, pady=10)

        # Cabeçalho
        frame_header = ctk.CTkFrame(frame_tabela, fg_color=("gray85", "gray25"), height=28)
        frame_header.pack(fill="x")
        frame_header.pack_propagate(False)

        for i, (col, w) in enumerate(zip(self.COLUNAS, self.COL_WIDTHS)):
            ctk.CTkLabel(
                frame_header, text=col,
                font=("Helvetica", 11, "bold"),
                width=w, anchor="w"
            ).grid(row=0, column=i, padx=(8 if i == 0 else 2, 2), pady=3, sticky="w")

        # Linhas com scroll
        self.frame_linhas = ctk.CTkScrollableFrame(frame_tabela, fg_color="transparent")
        self.frame_linhas.pack(fill="both", expand=True)

        # ── Status ─────────────────────────────────────────────────────
        self.lbl_status = ctk.CTkLabel(self, text="", font=("Helvetica", 11), text_color="gray")
        self.lbl_status.pack(pady=(0, 10))

    # ------------------------------------------------------------------
    # Lógica
    # ------------------------------------------------------------------

    _MAPA_TIPO_LABEL = {"RE": "Relatório", "IN": "Informativo"}
    _MAPA_PER_LABEL  = {"U": "Único", "Mi": "Minuto", "H": "Hora", "D": "Dia", "M": "Mês"}

    def _buscar(self):
        nome_filtro = self.entry_nome.get().strip() or None
        id_filtro = None

        id_raw = self.entry_id.get().strip()
        if id_raw:
            try:
                id_filtro = int(id_raw)
            except ValueError:
                msg_warning("O ID deve ser um número inteiro.")
                return

        if nome_filtro is None and id_filtro is None:
            msg_warning("Informe ao menos o nome ou o ID para buscar.")
            return

        try:
            resultados = self.db.buscar_rotinas(nome=nome_filtro, id_rotina=id_filtro)
        except Exception as e:
            msg_error(f"Erro na busca: {e}")
            return

        self._renderizar_resultados(resultados)

    def _renderizar_resultados(self, rotinas: list[dict]):
        for widget in self.frame_linhas.winfo_children():
            widget.destroy()

        if not rotinas:
            ctk.CTkLabel(
                self.frame_linhas, text="Nenhuma rotina encontrada.",
                font=("Helvetica", 11), text_color="gray"
            ).pack(pady=20)
            self.lbl_status.configure(text="")
            return

        for i, rotina in enumerate(rotinas):
            bg = ("gray92", "gray20") if i % 2 == 0 else ("gray86", "gray17")
            linha = ctk.CTkFrame(self.frame_linhas, fg_color=bg, height=28, cursor="hand2")
            linha.pack(fill="x", pady=1)
            linha.pack_propagate(False)

            valores = (
                str(rotina["id_rotina"]),
                rotina["nome"],
                self._MAPA_TIPO_LABEL.get(rotina["tipo"], rotina["tipo"]),
                self._MAPA_PER_LABEL.get(rotina["periodo"], rotina["periodo"]),
                rotina["ativo"],
            )
            for j, (val, w) in enumerate(zip(valores, self.COL_WIDTHS)):
                ctk.CTkLabel(
                    linha, text=val,
                    font=("Helvetica", 11),
                    width=w, anchor="w"
                ).grid(row=0, column=j, padx=(8 if j == 0 else 2, 2), pady=3, sticky="w")

            # Clique em qualquer parte da linha abre edição
            linha.bind("<Button-1>", lambda e, r=rotina: self._abrir_edicao(r))
            for child in linha.winfo_children():
                child.bind("<Button-1>", lambda e, r=rotina: self._abrir_edicao(r))

        total = len(rotinas)
        self.lbl_status.configure(
            text=f"{total} rotina{'s' if total > 1 else ''} encontrada{'s' if total > 1 else ''}."
        )

    def _abrir_edicao(self, rotina: dict):
        JanelaEdicao(self, rotina, self.db)


# ===========================================================================
# Janela de Edição
# ===========================================================================

class JanelaEdicao(ctk.CTkToplevel):
    """
    Janela de edição de uma rotina existente.
    Carrega todos os campos com os dados atuais e salva via DB.atualizar_rotina.
    """

    def __init__(self, master: JanelaBusca, rotina: dict, db: DB):
        super().__init__(master)
        self.db = db
        self.rotina = rotina
        self.id_rotina = rotina["id_rotina"]

        self.mapa_tipos = {"Relatório": "RE", "Informativo": "IN"}
        self.mapa_periodos = {"Único": "U", "Minuto": "Mi", "Hora": "H", "Dia": "D", "Mês": "M"}
        self._mapa_tipo_inv = {v: k for k, v in self.mapa_tipos.items()}
        self._mapa_per_inv  = {v: k for k, v in self.mapa_periodos.items()}

        # Estado de arquivos (espelha app.py)
        self.dict_corpo: dict = dict(rotina.get("corpos", {}))
        self.dict_links: dict = dict(rotina.get("links", {}))
        self.dict_anexo: dict = dict(rotina.get("anexos", {}))
        self.arq_sql: str | None = None

        self.title(f"Editar Rotina — ID {self.id_rotina}")
        self.geometry("660x560")
        self.minsize(660, 500)
        self.resizable(False, True)
        self.grab_set()
        self.focus_set()

        self._build_vars()
        self._build_ui()
        self._preencher_campos()

    # ------------------------------------------------------------------
    # Vars & propriedades
    # ------------------------------------------------------------------

    def _build_vars(self):
        self.var_nome        = ctk.StringVar()
        self.var_ativo       = ctk.StringVar(value="S")
        self.var_tipos_sel   = ctk.StringVar(value="Selecione")
        self.var_periodos_sel = ctk.StringVar(value="Selecione")

    @property
    def var_tipos(self) -> str:
        return self.mapa_tipos.get(self.var_tipos_sel.get(), "")

    @property
    def var_periodos(self) -> str:
        return self.mapa_periodos.get(self.var_periodos_sel.get(), "")

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------

    def _build_ui(self):
        self.main_container = ctk.CTkScrollableFrame(self, corner_radius=10)
        self.main_container.pack(fill="both", expand=True, padx=15, pady=15)

        self._ui_tipo_ativo()
        self._ui_nome()
        self._ui_periodo_intervalo()
        self._ui_datas()
        self._ui_destinatarios()
        self._ui_informativos()
        self._ui_consulta()
        self._ui_botao_salvar()

    def _ui_tipo_ativo(self):
        frame = ctk.CTkFrame(self.main_container, fg_color="transparent")
        frame.pack(fill="x", padx=10, pady=5)
        frame.columnconfigure(2, weight=1)

        ctk.CTkLabel(frame, text="Tipo", font=("Helvetica", 12, "bold")).grid(row=0, column=0, padx=10)
        self.box_tipos = ctk.CTkOptionMenu(
            frame, variable=self.var_tipos_sel,
            values=list(self.mapa_tipos.keys()),
            command=self._on_tipo_change
        )
        self.box_tipos.grid(row=0, column=1, padx=10)

        ctk.CTkLabel(frame, text="Ativo", font=("Helvetica", 12, "bold")).grid(row=0, column=3, padx=10)
        ctk.CTkRadioButton(frame, text="Sim", variable=self.var_ativo, value="S").grid(row=0, column=4, padx=5)
        ctk.CTkRadioButton(frame, text="Não", variable=self.var_ativo, value="N").grid(row=0, column=5, padx=5)

    def _ui_nome(self):
        frame = ctk.CTkFrame(self.main_container, fg_color="transparent")
        frame.pack(fill="x", padx=10, pady=5)
        ctk.CTkLabel(frame, text="Nome", font=("Helvetica", 12, "bold")).grid(row=0, column=0, padx=10, sticky="w")
        self.entry_nome = ctk.CTkEntry(frame, textvariable=self.var_nome, width=498,
                                       placeholder_text="Nome da rotina...")
        self.entry_nome.grid(row=0, column=1, padx=10)

    def _ui_periodo_intervalo(self):
        frame = ctk.CTkFrame(self.main_container, fg_color="transparent")
        frame.pack(fill="x", padx=10, pady=5)
        frame.columnconfigure(2, weight=1)

        ctk.CTkLabel(frame, text="Período", font=("Helvetica", 12, "bold")).grid(row=0, column=0, padx=10)
        self.box_periodos = ctk.CTkOptionMenu(
            frame, variable=self.var_periodos_sel,
            values=list(self.mapa_periodos.keys()),
            command=self._on_periodo_change
        )
        self.box_periodos.grid(row=0, column=1, padx=10)

        ctk.CTkLabel(frame, text="Intervalo", font=("Helvetica", 12, "bold")).grid(row=0, column=3, padx=10)
        self.entry_intervalo = ctk.CTkEntry(frame, width=100, placeholder_text="Ex: 1")
        self.entry_intervalo.grid(row=0, column=4, padx=10)

    def _ui_datas(self):
        self.frame_datas_master = ctk.CTkFrame(self.main_container, fg_color="transparent")
        self.frame_datas_master.pack(fill="x", padx=10, pady=5)

        ctk.CTkLabel(self.frame_datas_master, text="Início", font=("Helvetica", 12, "bold")).grid(
            row=0, column=0, padx=10)
        self.entry_data_inicial = ctk.CTkEntry(self.frame_datas_master, placeholder_text="DD/MM/AAAA")
        self.entry_data_inicial.grid(row=0, column=1, padx=5)
        self.entry_hora_inicial = ctk.CTkEntry(self.frame_datas_master, placeholder_text="HH:MM:SS", width=100)
        self.entry_hora_inicial.grid(row=0, column=2, padx=5)

        self.frame_data_final = ctk.CTkFrame(self.frame_datas_master, fg_color="transparent")
        ctk.CTkLabel(self.frame_data_final, text="Fim   ", font=("Helvetica", 12, "bold")).grid(
            row=0, column=0, padx=10)
        self.entry_data_final = ctk.CTkEntry(self.frame_data_final, placeholder_text="DD/MM/AAAA")
        self.entry_data_final.grid(row=0, column=1, padx=5)
        self.entry_hora_final = ctk.CTkEntry(self.frame_data_final, placeholder_text="HH:MM:SS", width=100)
        self.entry_hora_final.grid(row=0, column=2, padx=5)

    def _ui_destinatarios(self):
        frame = ctk.CTkFrame(self.main_container, fg_color="transparent")
        frame.pack(fill="x", padx=10, pady=5)
        ctk.CTkLabel(frame, text="Destinatários", font=("Helvetica", 12, "bold")).pack(anchor="nw", padx=10)
        self.entry_destinatarios = ctk.CTkTextbox(frame, height=80, width=580)
        self.entry_destinatarios.pack(padx=10, pady=5)

    def _ui_consulta(self):
        self.frame_consulta = ctk.CTkFrame(self.main_container)
        ctk.CTkLabel(self.frame_consulta, text="Consulta SQL",
                     font=("Helvetica", 12, "bold")).grid(row=0, column=0, padx=10, pady=10)
        ctk.CTkButton(self.frame_consulta, text="+", fg_color="#2ecc71",
                      command=self._get_consulta_sql, width=30).grid(row=0, column=1, padx=10)
        self.label_arq_consulta = ctk.CTkLabel(
            self.frame_consulta, text=' "seu_arquivo.sql" ', text_color="gray")
        self.label_arq_consulta.grid(row=0, column=2, padx=10)
        self.btn_limpa_consulta = ctk.CTkButton(
            self.frame_consulta, text="X", width=30, fg_color="#e74c3c",
            command=self._limpa_consulta_sql)

    def _ui_informativos(self):
        self.frame_informativos = ctk.CTkFrame(self.main_container)

        top_bar_anexos = ctk.CTkFrame(self.frame_informativos, fg_color="transparent")
        top_bar_anexos.pack(fill="x", padx=5, pady=5)
        ctk.CTkLabel(top_bar_anexos, text="Anexos", font=("Helvetica", 12, "bold")).pack(side="left", padx=10)
        ctk.CTkButton(top_bar_anexos, text="+", width=30, fg_color="#2ecc71", hover_color="#27ae60",
                      command=lambda: self._adiciona_anexo(self.dict_anexo, self.frame_anexos_lista)
                      ).pack(side="left")
        self.frame_anexos_lista = ctk.CTkFrame(self.frame_informativos, height=30)
        self.frame_anexos_lista.pack(fill="x", padx=10, pady=5)

        top_bar_corpo = ctk.CTkFrame(self.frame_informativos, fg_color="transparent")
        top_bar_corpo.pack(fill="x", padx=5, pady=5)
        ctk.CTkLabel(top_bar_corpo, text="Corpo", font=("Helvetica", 12, "bold")).pack(side="left", padx=10)
        ctk.CTkButton(top_bar_corpo, text="+", width=30, fg_color="#2ecc71", hover_color="#27ae60",
                      command=lambda: self._adiciona_anexo(self.dict_corpo, self.frame_corpo_lista)
                      ).pack(side="left")
        self.frame_corpo_lista = ctk.CTkFrame(self.frame_informativos, height=30)
        self.frame_corpo_lista.pack(fill="x", padx=10, pady=5)

    def _ui_botao_salvar(self):
        frame = ctk.CTkFrame(self.main_container, fg_color="transparent")
        frame.pack(fill="x", side="bottom", pady=20)
        ctk.CTkButton(
            frame, text="SALVAR ALTERAÇÕES",
            command=self._salvar, font=("Helvetica", 14, "bold"), height=45
        ).pack(fill="x", padx=50)

    # ------------------------------------------------------------------
    # Preenchimento inicial
    # ------------------------------------------------------------------

    def _preencher_campos(self):
        r = self.rotina
        self.var_nome.set(r.get("nome", ""))
        self.var_ativo.set(r.get("ativo", "S"))

        tipo_label = self._mapa_tipo_inv.get(r.get("tipo", ""), "Selecione")
        self.var_tipos_sel.set(tipo_label)
        self._on_tipo_change(tipo_label)

        per_label = self._mapa_per_inv.get(r.get("periodo", ""), "Selecione")
        self.var_periodos_sel.set(per_label)
        self._on_periodo_change(per_label)

        if r.get("intervalo"):
            self.entry_intervalo.insert(0, str(r["intervalo"]))

        if r.get("dta_inicial"):
            d = r["dta_inicial"]
            self.entry_data_inicial.insert(0, d.strftime("%d/%m/%Y"))
            self.entry_hora_inicial.insert(0, d.strftime("%H:%M:%S"))

        if r.get("dta_final"):
            d = r["dta_final"]
            self.entry_data_final.insert(0, d.strftime("%d/%m/%Y"))
            self.entry_hora_final.insert(0, d.strftime("%H:%M:%S"))

        if r.get("destinatarios"):
            self.entry_destinatarios.insert("1.0", "; ".join(r["destinatarios"]))

        # Atualiza listas de arquivos já existentes no banco
        if self.dict_corpo:
            self._atualiza_label_anexos(self.dict_corpo, self.frame_corpo_lista)
        if self.dict_anexo:
            self._atualiza_label_anexos(self.dict_anexo, self.frame_anexos_lista)

    # ------------------------------------------------------------------
    # Callbacks de tipo / período
    # ------------------------------------------------------------------

    def _on_tipo_change(self, choice: str):
        tipo = self.mapa_tipos.get(choice, "")
        if tipo == "RE":
            if self.frame_informativos.winfo_ismapped():
                self.frame_informativos.pack_forget()
            self.frame_consulta.pack(fill="both", expand=True, padx=10, pady=5)
        elif tipo == "IN":
            if self.frame_consulta.winfo_ismapped():
                self.frame_consulta.pack_forget()
            if not self.frame_informativos.winfo_ismapped():
                self.frame_informativos.pack(fill="both", expand=True, padx=10, pady=5)

    def _on_periodo_change(self, choice: str):
        periodo = self.mapa_periodos.get(choice, "")
        if periodo == "U":
            self.frame_data_final.grid_forget()
        else:
            self.frame_data_final.grid(row=1, column=0, columnspan=4, pady=5)

    # ------------------------------------------------------------------
    # Consulta SQL
    # ------------------------------------------------------------------

    def _get_consulta_sql(self):
        file_path = askopenfilename(filetypes=[("SQL", "*.sql")])
        if file_path:
            self.arq_sql = file_path
            name = Path(file_path).name
            display = f" {name[:15]}...{Path(file_path).suffix} " if len(name) > 20 else f" {name} "
            self.label_arq_consulta.configure(text=display, text_color="#2ecc71")
            self.btn_limpa_consulta.grid(row=0, column=3, padx=10, sticky="e")

    def _limpa_consulta_sql(self):
        self.arq_sql = None
        self.label_arq_consulta.configure(text=' "seu_arquivo.sql" ', text_color="gray")
        self.btn_limpa_consulta.grid_forget()

    # ------------------------------------------------------------------
    # Anexos / Corpo (espelham app.py)
    # ------------------------------------------------------------------

    def _adiciona_anexo(self, target_dict: dict, target_frame):
        try:
            file_path = askopenfilename()
            if not file_path:
                return
            if target_dict is self.dict_corpo:
                extensoes_validas = ('.docx', '.doc', '.jpeg', '.png', '.jpg')
                if Path(file_path).suffix.lower() not in extensoes_validas:
                    raise Exception(
                        "O corpo deve conter apenas imagens (.jpeg, .png, .jpg) "
                        "ou documentos Word (.docx, .doc)."
                    )
            novo_id = max(target_dict.keys()) + 1 if target_dict else 1
            target_dict[novo_id] = file_path
            self._atualiza_label_anexos(target_dict, target_frame)
        except Exception as e:
            msg_warning(e)

    def _exclue_anexo(self, key: int, target_dict: dict, target_frame):
        target_dict.pop(key, None)
        self.dict_links.pop(key, None)
        self._atualiza_label_anexos(target_dict, target_frame)

    def _mover_corpo(self, key: int, direcao: int):
        chaves = list(self.dict_corpo.keys())
        idx = chaves.index(key)
        novo_idx = idx + direcao
        if novo_idx < 0 or novo_idx >= len(chaves):
            return
        chaves[idx], chaves[novo_idx] = chaves[novo_idx], chaves[idx]
        novo_corpo, novo_links = {}, {}
        for nova_chave, velha_chave in enumerate(chaves, start=1):
            novo_corpo[nova_chave] = self.dict_corpo[velha_chave]
            if velha_chave in self.dict_links:
                novo_links[nova_chave] = self.dict_links[velha_chave]
        self.dict_corpo.clear(); self.dict_corpo.update(novo_corpo)
        self.dict_links.clear(); self.dict_links.update(novo_links)
        self._atualiza_label_anexos(self.dict_corpo, self.frame_corpo_lista)

    def _atualiza_label_anexos(self, target_dict: dict, target_frame):
        for widget in target_frame.winfo_children():
            widget.destroy()

        chaves = list(target_dict.keys())
        is_corpo = target_dict is self.dict_corpo

        for k, v in target_dict.items():
            suffix = Path(str(v)).suffix.lower()
            name = Path(str(v)).name
            short_name = f"{name[:15]}~{suffix}" if len(name) > 20 else name

            f = ctk.CTkFrame(target_frame, fg_color="transparent")
            f.pack(fill="x", pady=2)

            if is_corpo:
                idx = chaves.index(k)
                ctk.CTkButton(f, text="▲", width=22, height=20,
                              fg_color="#3498db", hover_color="#2980b9",
                              state="normal" if idx > 0 else "disabled",
                              command=lambda k=k: self._mover_corpo(k, -1)
                              ).pack(side="left", padx=(5, 1))
                ctk.CTkButton(f, text="▼", width=22, height=20,
                              fg_color="#3498db", hover_color="#2980b9",
                              state="normal" if idx < len(chaves) - 1 else "disabled",
                              command=lambda k=k: self._mover_corpo(k, +1)
                              ).pack(side="left", padx=(1, 5))

            ctk.CTkLabel(f, text=short_name, font=("Helvetica", 11)).pack(side="left", padx=5)

            ctk.CTkButton(f, text="X", width=20, height=20,
                          fg_color="#e74c3c", hover_color="#c0392b",
                          command=lambda k=k, d=target_dict, fr=target_frame: self._exclue_anexo(k, d, fr)
                          ).pack(side="right", padx=5)

            if is_corpo and suffix in ('.jpeg', '.png', '.jpg'):
                self._create_hiperlink_entry(f, k)

    def _create_hiperlink_entry(self, master_frame, key):
        valor = self.dict_links.get(key, "")
        entry = ctk.CTkEntry(master_frame, placeholder_text="Insira o hiperlink...",
                             height=22, font=("Helvetica", 10))
        if valor:
            entry.insert(0, valor)
        entry.pack(side="left", fill="x", expand=True, padx=10)
        entry.bind("<KeyRelease>", lambda e, en=entry: self.dict_links.__setitem__(key, en.get()))

    # ------------------------------------------------------------------
    # Salvar
    # ------------------------------------------------------------------

    def _salvar(self):
        try:
            self._validar()

            dta_inicial = dt.combine(
                dt.strptime(self.entry_data_inicial.get(), "%d/%m/%Y").date(),
                parse_hora(self.entry_hora_inicial.get())
            )

            dta_final = None
            if self.entry_data_final.get().strip():
                dta_final = dt.combine(
                    dt.strptime(self.entry_data_final.get(), "%d/%m/%Y").date(),
                    parse_hora(self.entry_hora_final.get())
                )

            destinatarios = (
                self.entry_destinatarios.get("1.0", "end-1c")
                .replace(";", " ").split()
            )

            consulta = None
            if self.arq_sql:
                with open(self.arq_sql, "r", encoding="utf-8") as f:
                    consulta = f.read()
            elif self.rotina.get("consulta"):
                consulta = self.rotina["consulta"]

            intervalo = int(self.entry_intervalo.get()) if self.entry_intervalo.get().strip() else 0

            self.db.atualizar_rotina(
                id_rotina=self.id_rotina,
                nome=self.var_nome.get().title(),
                periodo=self.var_periodos,
                intervalo=intervalo,
                dta_inicial=dta_inicial,
                dta_final=dta_final,
                consulta=consulta,
                tipo=self.var_tipos,
                ativo=self.var_ativo.get(),
                destinatarios=destinatarios,
                corpos=self.dict_corpo or None,
                hiperlinks=self.dict_links or None,
                anexos=self.dict_anexo or None,
            )

            showinfo(title="Rotinas",
                     message=f'Rotina "{self.var_nome.get()}" atualizada com sucesso.')
            self.destroy()

        except ValueError as e:
            msg_error(f"Formato de data inválido. Use DD/MM/AAAA. Detalhe: {e}")
        except Exception as e:
            msg_error(f"Erro ao salvar: {e}")

    def _validar(self):
        if self.var_tipos not in self.mapa_tipos.values():
            raise Exception("Tipo de rotina não informado.")
        if not self.var_nome.get().strip():
            raise Exception("Favor informar o nome da rotina.")
        if self.var_periodos not in self.mapa_periodos.values():
            raise Exception("Favor selecionar um Período.")
        if not self.entry_intervalo.get().strip() and self.var_periodos != "U":
            raise Exception("Favor informar o Intervalo.")
        if self.entry_intervalo.get().strip():
            try:
                int(self.entry_intervalo.get())
            except ValueError:
                raise Exception("O intervalo deve ser um número inteiro.")
        if not self.entry_data_inicial.get().strip():
            raise Exception("Favor informar a data de início.")
        if not self.entry_destinatarios.get("1.0", "end-1c").strip():
            raise Exception("Favor informar o(s) destinatário(s).")
        if not self.arq_sql and not self.rotina.get("consulta") and self.var_tipos == "RE":
            raise Exception("Insira o arquivo de consulta SQL.")
        if not self.dict_anexo and not self.dict_corpo:
            raise Exception("Favor inserir pelo menos um anexo ou corpo para o e-mail.")
        count_doc = sum(1 for v in self.dict_corpo.values()
                        if str(v).lower().endswith(('.docx', '.doc')))
        if count_doc > 1:
            raise Exception("O Corpo deve conter apenas um arquivo .docx ou .doc.")


if __name__ == "__main__":
    app = App()
    app.mainloop()