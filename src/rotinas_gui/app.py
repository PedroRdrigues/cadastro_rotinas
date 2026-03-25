import tkinter as tk
from idlelib.configdialog import font_sample_text
from pathlib import Path
from tkinter.messagebox import showerror
from typing import Optional, Any
from datetime import datetime as dt
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from tkinter.filedialog import askopenfile



class Funcs:
    def teste(self, *args):
        print(self.var_tipos.get())
        print(self.var_nome.get())
        print(self.var_periodos.get())
        print(self.var_ativo.get())
        data_i = self.entry_data_inicial.get_date()
        hora_i = self.parse_hora(self.entry_hora_inicial.get())
        data_inicial_completa = dt.combine(data_i.date(), hora_i)
        print(data_inicial_completa)

        data_f = self.entry_data_final.get_date()
        hora_f = self.parse_hora(self.entry_hora_final.get())
        data_final_completa = dt.combine(data_f.date(), hora_f)
        print(data_final_completa)
        print(self.entry_destinatarios.get("1.0", "end-1c"))

        try:
            print(self.arq_sql)
        except Exception as e:
            self.msg_error(e)




    def parse_hora(self, entry_value: str):
        try:
            entry_value = int(entry_value)
        except:
            self.entry_hora_inicial.delete(0, END)
            self.entry_hora_final.delete(0, END)

            self.entry_hora_inicial = self.create_placeholder_entry(self.frame_datas, placeholder_text="HH:MM:SS", textvariable=self.var_hora_inicial)
            self.entry_hora_final = self.create_placeholder_entry(self.frame_datas, placeholder_text="HH:MM:SS", textvariable=self.var_hora_final)
            entry_value = "0"

        partes = entry_value.split(':')

        # Garante 3 elementos (hora, minuto, segundo)
        partes = (partes + ['0', '0', '0'])[:3]

        try:
            hora, minuto, segundo = [int(p) for p in partes]
            hora_formatada = f"{hora:02}"
            minuto_formatado = f"{minuto:02}"
            segundo_formatado = f"{segundo:02}"

            hora_str = f"{hora_formatada}:{minuto_formatado}:{segundo_formatado}"
            return dt.strptime(hora_str, '%H:%M:%S').time()

        except ValueError:
            # Caso usuário digite algo inválido
            return None

    def on_focus_in(self, event, entry, placeholder):
        """Handles removing the placeholder text on focus."""
        if entry.get() == placeholder:
            entry.delete(0, END)
            entry.configure(style='primary.TEntry')  # Active color

    def on_focus_out(self, event, entry, placeholder):
        """Handles adding the placeholder text back if empty."""
        if entry.get() == "":
            entry.insert(0, placeholder)
            entry.configure(style='info.TEntry')  # Placeholder color

    def create_placeholder_entry(self,master, placeholder_text, textvariable: Optional[Any] = None):
        """Creates a themed Entry with placeholder functionality."""
        if textvariable is None:
            entry = ttk.Entry(master)
        else:
            entry = ttk.Entry(master, textvariable=textvariable)

        entry.insert(0, placeholder_text)
        entry.configure(style='info.TEntry')  # Use a muted style for placeholder

        # Bind events
        entry.bind('<FocusIn>', lambda e: self.on_focus_in(e, entry, placeholder_text))
        entry.bind('<FocusOut>', lambda e: self.on_focus_out(e, entry, placeholder_text))

        return entry

    def get_consulta_sql(self):
        self.arq_sql = askopenfile("r", filetypes=(("SQL", "*.sql"), ))

        try:
            with open(self.arq_sql.name, "r") as f:
                print(f.read())

            arq_sql_name = f" {Path(self.arq_sql.name).name} " \
                    if len(Path(self.arq_sql.name).name) <= 20 else f" {Path(self.arq_sql.name).name[:15]}~.sql "
            print(arq_sql_name)
            self.label_arq_consulta.config(text=arq_sql_name, bootstyle='inverse-success', borderwidth=2, relief="solid")

            self.btn_limpa_arq_consulta = ttk.Button(self.frame_consulta, text="X", command=self.limpa_consulta_sql,
                                                     bootstyle="danger")
            self.btn_limpa_arq_consulta.grid(row=0, column=3, padx=10, pady=10, sticky=tk.E)
        except:
            self.limpa_consulta_sql()

    def limpa_consulta_sql(self):
        try:
            self.btn_limpa_arq_consulta.destroy()
        except:
            pass
        finally:
            self.arq_sql = None
            self.label_arq_consulta.config(text=" \"seu_arquivo.sql\" ", bootstyle='default', borderwidth=0, relief="")

    def select_frame_from_type(self, *args):
        if self.var_tipos.get() == "Relatório":
            print("relatório")
            self.cria_frame_consulta()

        elif self.var_tipos.get() == "Informativo":
            print("Informativo")
            self.cria_frame_informativo()

    def select_data_final_from_type(self, *args):
        if self.var_periodos.get() == 'Único':
            self.entry_data_final.grid_forget()
            self.entry_hora_final.grid_forget()
            self.label_hora_final.grid_forget()
            self.label_data_final.grid_forget()
        else:
            self.label_hora_final.grid(row=1, column=2, padx=10, pady=10, sticky=tk.W)
            self.entry_hora_final.grid(row=1, column=3, padx=10, pady=10, sticky=tk.W)
            self.entry_data_final.grid(row=1, column=1, padx=10, pady=10, sticky=tk.W)

    def limpa_data_final(self):
        self.entry_data_final.grid_forget()
        self.entry_hora_final.grid_forget()
        self.label_hora_final.grid_forget()
        self.label_data_final.grid_forget()

    def cria_frame_consulta(self):
        self.frame_informativos.pack_forget()
        self.frame_cadastrar.pack_forget()
        self.frame_consulta.pack(fill=BOTH, expand=YES)
        self.frame_cadastrar.pack(fill=BOTH, expand=YES)

    def cria_frame_informativo(self):
        self.limpa_consulta_sql()
        self.frame_consulta.pack_forget()
        self.frame_cadastrar.pack_forget()
        self.frame_informativos.pack(fill=BOTH, expand=YES)
        self.frame_cadastrar.pack(fill=BOTH, expand=YES)


    def msg_error(self, err):
        showerror(
            title="Mensagem de erro",
            message=err
        )



class App(Funcs):
    DEFAULT_FONT = ("Helvetica", 12, "bold")

    def __init__(self):
        self.root = ttk.Window(themename="superhero", title="Rotinas")
        self.root.resizable(False, False)
        self.layout()
        self.root.mainloop()

    def layout(self):
        self.func_frame_tipo_ativo()
        self.func_frame_nome()
        self.func_frame_periodo_intervalo()
        self.func_frame_datas()
        self.func_frame_destinatarios()
        self.func_frame_informativos()
        self.func_frame_consulta()
        self.func_frame_cadastar()

    def func_frame_tipo_ativo(self):
        # Tipo:
        self.frame_tipo_ativo = ttk.Labelframe(self.root)
        self.frame_tipo_ativo.pack(fill=BOTH, expand=YES)
        self.frame_tipo_ativo.columnconfigure([2,6], weight=1)


        self.label_tipo = ttk.Label(self.frame_tipo_ativo, text='Tipo', font=self.DEFAULT_FONT)
        self.label_tipo.grid(row=0, column=0, padx=10, pady=10, sticky=tk.W)

        options_list_tipo = ["Relatório", "Informativo"]
        self.var_tipos = ttk.StringVar(self.frame_tipo_ativo)
        self.var_tipos.set("Selecione")
        self.var_tipos.trace_add('write', self.select_frame_from_type)

        self.box_tipos = tk.OptionMenu(self.frame_tipo_ativo, self.var_tipos, *options_list_tipo)
        self.box_tipos.grid(row=0, column=1, padx=10, pady=10, sticky=tk.E)

        # Ativo
        self.label_ativo = ttk.Label(self.frame_tipo_ativo, text='Ativo', font=self.DEFAULT_FONT)
        self.label_ativo.grid(row=0, column=3, padx=10, pady=10, sticky=tk.W)

        self.var_ativo = tk.StringVar(value="S")

        self.radio_ativo = ttk.Radiobutton(self.frame_tipo_ativo, text="Sim", variable=self.var_ativo, value="S", bootstyle="outline-toolbutton")
        self.radio_ativo.grid(row=0, column=4, padx=10, pady=10)

        self.radio_ativo = ttk.Radiobutton(self.frame_tipo_ativo, text="Não", variable=self.var_ativo, value="N", bootstyle="outline-toolbutton")
        self.radio_ativo.grid(row=0, column=5, padx=10, pady=10, sticky=tk.W)

    def func_frame_nome(self):
        # Nome
        self.frame_nome = ttk.Labelframe(self.root)
        self.frame_nome.pack(fill=BOTH, expand=YES)

        self.label_nome = ttk.Label(self.frame_nome, text='Nome', font=self.DEFAULT_FONT)
        self.label_nome.grid(row=1, column=0, padx=10, pady=10, sticky=tk.W)

        self.var_nome = ttk.StringVar(self.frame_nome)
        self.entry_nome = ttk.Entry(self.frame_nome, textvariable=self.var_nome, width=63)
        self.entry_nome.grid(row=1, column=1, padx=10, pady=10, sticky=tk.W)

    def func_frame_periodo_intervalo(self):
        # Período:
        self.frame_periodo_intervalo = ttk.Labelframe(self.root)
        self.frame_periodo_intervalo.pack(fill=BOTH, expand=YES)

        self.label_periodo = ttk.Label(self.frame_periodo_intervalo, text='Período', font=self.DEFAULT_FONT)
        self.label_periodo.grid(row=0, column=0, padx=10, pady=10, sticky=tk.W)

        options_list_periodo = ["Único", "Hora", "Dia", "Mês"]
        self.var_periodos = ttk.StringVar(self.frame_periodo_intervalo)
        self.var_periodos.set(options_list_periodo[0])
        self.var_periodos.trace_add('write', self.select_data_final_from_type)

        self.box_periodos = tk.OptionMenu(self.frame_periodo_intervalo, self.var_periodos, *options_list_periodo)
        self.box_periodos.grid(row=0, column=1, padx=10, pady=10, sticky=tk.W)

        # Intervalo
        self.label_intervalo = ttk.Label(self.frame_periodo_intervalo, text='Intervalo', font=self.DEFAULT_FONT)
        self.label_intervalo.grid(row=0, column=3, padx=10, pady=10, sticky=tk.W)

        self.var_intervalo = ttk.IntVar(self.frame_periodo_intervalo)
        self.entry_intervalo = ttk.Entry(self.frame_periodo_intervalo, textvariable=self.var_intervalo, width=15)
        self.entry_intervalo.grid(row=0, column=4, padx=10, pady=10, sticky=tk.W)
        self.frame_periodo_intervalo.columnconfigure(2, weight=1)

    def func_frame_datas(self):
        # Datas:
        self.frame_datas = ttk.Labelframe(self.root)
        self.frame_datas.pack(fill=BOTH, expand=YES)

        # Data inicial
        self.label_data_inicial = ttk.Label(self.frame_datas, text='Data Inicial', font=self.DEFAULT_FONT)
        self.label_data_inicial.grid(row=0, column=0, padx=10, pady=10, sticky=tk.W)
        self.entry_data_inicial = ttk.DateEntry(self.frame_datas, bootstyle="info")
        self.entry_data_inicial.grid(row=0, column=1, padx=10, pady=10, sticky=tk.W)

        # Hora inicial
        self.var_hora_inicial = ttk.StringVar(self.frame_nome)

        self.label_hora_inicial = ttk.Label(self.frame_datas, text='Hora', font=self.DEFAULT_FONT)
        self.label_hora_inicial.grid(row=0, column=2, padx=10, pady=10, sticky=tk.W)
        self.entry_hora_inicial = self.create_placeholder_entry(self.frame_datas, placeholder_text="HH:MM:SS", textvariable=self.var_hora_inicial)
        self.entry_hora_inicial.grid(row=0, column=3, padx=10, pady=10, sticky=tk.W)


        # Data Final
        self.label_data_final = ttk.Label(self.frame_datas, text='Data Final', font=self.DEFAULT_FONT)
        self.label_data_final.grid(row=1, column=0, padx=10, pady=10, sticky=tk.W)
        self.entry_data_final = ttk.DateEntry(self.frame_datas, bootstyle="info")
        self.entry_data_final.grid(row=1, column=1, padx=10, pady=10, sticky=tk.W)

        # Hora final
        self.var_hora_final = ttk.StringVar(self.frame_nome)

        self.label_hora_final = ttk.Label(self.frame_datas, text='Hora', font=self.DEFAULT_FONT)
        self.label_hora_final.grid(row=1, column=2, padx=10, pady=10, sticky=tk.W)
        self.entry_hora_final = self.create_placeholder_entry(self.frame_datas, placeholder_text="HH:MM:SS", textvariable=self.var_hora_final)
        self.entry_hora_final.grid(row=1, column=3, padx=10, pady=10, sticky=tk.W)

    def func_frame_destinatarios(self):
        # Destinatários
        self.frame_destinatarios = ttk.Labelframe(self.root)
        self.frame_destinatarios.pack(fill=BOTH, expand=YES)

        self.label_destinatarios = ttk.Label(self.frame_destinatarios, text='Destinatários', font=self.DEFAULT_FONT)
        self.label_destinatarios.grid(row=0, column=0, padx=10, pady=10, sticky=tk.N)

        self.var_destinatarios = ttk.StringVar(self.frame_destinatarios)
        self.entry_destinatarios = ttk.Text(self.frame_destinatarios, height=5, width=53)
        self.entry_destinatarios.grid(row=0, column=1, padx=10, pady=10, sticky=tk.W)

    def func_frame_consulta(self):
        # Consulta SQL
        self.frame_consulta = ttk.Labelframe(self.root)
        # self.frame_consulta.pack(fill=BOTH, expand=YES)

        self.label_consulta = ttk.Label(self.frame_consulta, text='Consulta', font=self.DEFAULT_FONT)
        self.label_consulta.grid(row=0, column=0, padx=10, pady=10, sticky=tk.W)

        self.btn_consulta = ttk.Button(self.frame_consulta, text="Importar Consulta", command=self.get_consulta_sql)
        self.btn_consulta.grid(row=0, column=1, padx=10, pady=10, sticky=tk.W)

        self.label_arq_consulta = ttk.Label(self.frame_consulta, text=" \"seu_arquivo.sql\" ", font=self.DEFAULT_FONT)
        self.label_arq_consulta.grid(row=0, column=2, padx=10, pady=10, sticky=tk.NSEW, ipady=5)


    def func_frame_informativos(self):
        self.frame_informativos = ttk.Labelframe(self.root)
        # self.frame_informativos.pack(fill=BOTH, expand=YES)

        self.label_anexos = ttk.Label(self.frame_informativos, text='Anexo', font=self.DEFAULT_FONT)
        self.label_anexos.grid(row=0, column=0, padx=10, pady=10, sticky=tk.W)




    def func_frame_cadastar(self):
        # Cadastrar
        self.frame_cadastrar = ttk.Labelframe(self.root)
        self.frame_cadastrar.pack(fill=BOTH, expand=YES)

        self.btn_cadastar = ttk.Button(self.frame_cadastrar, text="Cadastar", command=self.teste, width=30)
        self.btn_cadastar.grid(row=0, column=0, padx=10, pady=10, sticky=tk.N)
        self.frame_cadastrar.columnconfigure(0, weight=1)



App()


