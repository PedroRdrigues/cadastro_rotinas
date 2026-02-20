# 🕒 Routine Manager Service

Um motor de execução de rotinas robusto que transforma scripts em serviços resilientes do Windows. O sistema monitora agendamentos em banco de dados, gerencia execuções concorrentes via Threads e automatiza o report de resultados por e-mail.

## 🚀 Funcionalidades

* **Serviço Windows:** Execução contínua em segundo plano via NSSM.
* **Multithreading:** Processamento paralelo de rotinas para evitar gargalos.
* **Tipos de Rotina:** Suporte nativo para:
* 📊 **Relatórios:** Geração e envio de dados.
* 🔔 **Informativos:** Alertas e comunicações.
* ⚡ **Triggers:** Disparos de eventos em outros sistemas.


* **Notificação Automática:** Sistema de mailing integrado pós-execução.

## 🛠️ Arquitetura do Banco de Dados

O serviço baseia-se em duas tabelas principais:

1. **`cadastro_rotinas`**: Contém o nome, tipo, comando de execução, status (ativo/inativo) e o cronograma/trigger.
2. **`email_rotinas`**: Lista de destinatários vinculados a cada rotina.

## ⚙️ Como Funciona

O fluxo de operação segue o ciclo abaixo:

1. **Polling:** O serviço consulta a tabela `cadastro_rotinas` em intervalos definidos.
2. **Verificação:** Filtra rotinas ativas cuja hora de execução coincide com o `now()`.
3. **Processamento:** Para cada rotina encontrada, o sistema abre uma **Thread dedicada**. Isso permite que um relatório pesado não atrase um informativo simples.
4. **Conclusão:** Após o término da tarefa, o serviço busca os contatos na `email_rotinas` e dispara o log/status da operação.

## 💻 Instalação e Configuração

### Pré-requisitos

* [NSSM (Non-Sucking Service Manager)](https://nssm.cc/)
* Banco de Dados (Configurar string de conexão no `.env`)

### Passos para Instalação

1. **Clone o repositório:**
```bash
git clone https://github.com/PedroRdrigues/cadastro_rotinas.git

```


2. **Configure as variáveis de ambiente:**
Crie um arquivo `.env` na raiz seguindo o modelo `.env.example`.
3. **Instale como Serviço Windows:**
Abra o terminal como Administrador e utilize o NSSM:
```bash
nssm install RotinasService
nssm edit RotinasService
nssm start RotinasService

```



## 📊 Estrutura de Tipos

| Tipo | Descrição                                                | Ação Pós-Execução                |
| --- |----------------------------------------------------------|----------------------------------|
| **Relatório** | Criação de relatórios em Excel a partir de consultas sql | Envio de anexo via E-mail        |
| **Informativo** | Verificação de status simples                            | Envio de anexos/corpo via E-mail |
| **Trigger** | Execução de Procedure ou script externo                  | Confirmação de disparo           |

---

**Desenvolvido por [Pedro Rodrigues]**
*Conecte-se comigo no [LinkedIn*](www.linkedin.com/in/pedrordrigues)
