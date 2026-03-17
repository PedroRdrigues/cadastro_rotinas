# 🕒 Routine Manager Service

Um motor de execução de rotinas robusto que transforma scripts em serviços resilientes do Windows. O sistema monitora agendamentos em banco de dados, gerencia execuções concorrentes via Threads e automatiza o report de resultados por e-mail.

## Sumário
- [Funcionalidades](#Funcionalidades)
- [Arquitetura do Banco de Dados](#arquitetura-do-banco-de-dados)
- [Como Funciona](#como-funciona)
- [Instalação e Configuração](#instalação-e-configuração)
- [Estrutura de Tipos](#estrutura-de-tipos)


## 🚀 Funcionalidades

- **Serviço Windows:** Execução contínua em segundo plano via NSSM.
- **Multithreading:** Processamento paralelo de rotinas para evitar gargalos.
- **Tipos de Rotina:** Suporte nativo para:
  - 📊 **Relatórios:** Geração e envio de dados.
  - 🔔 **Informativos:** Alertas e comunicações.
  - ⚡ **Triggers:** Disparos de eventos em outros sistemas.


- **Notificação Automática:** Sistema de mailing integrado pós-execução.

## 🛠️ Arquitetura do Banco de Dados

O serviço baseia-se em três tabelas principais:

### **`cadastro_rotinas`**: Configurações das Rotinas
- `id_rotina` : Identificador único de cada rotina. **(SEQUENCE)**;
- `nome` : Nome da Rotina. **(OBRIGATÓRIO)**;
- `periodo` : Definição do período de execução, podendo ser Mi -> MINUTOS / H -> HORAS / D-> DIAS / M -> MESES / U -> UNICO. **(OBRIGATÓRIO)**;
- `intervalo` : Definição do intervalo entre cada execução referente ao período. Ex: período:D; intervalo:10. Dessa forma a rotina será executada a cada 10 dias;
- `dta_inicial` : Data da primeira execução. **(OBRIGATÓRIO)**;
- `dta_proxiam` : Data da próxima execução. Esse campo é atualizado automaticamente com os dados de período e intervalo;
- `dta_final` : Data da ultima execução;
- `consulta` : Consulta sql para execução de relatórios.
- `enviado` : Informa se o último envio foi realizado com sucesso;
- `status` : Informa se a rotina está em execução -> E, ou se já foi finalizou a execução -> F
- `tipo` : Define o tipo da rotina, podendo ser  RE -> RELATORIO com anexo / IN -> INFORMATIVO / TRG -> TRIGGER. **(OBRIGATÓRIO)**;
- `ativo` : Define se a rotina está ativa S ou não N. **(OBRIGATÓRIO)**;
- `tent_erro` : Quantidade de execução consecutivas com erro. Após a 5ª execução a rotina é inativada.


### **`email_rotinas`**: Lista de destinatários vinculados a cada rotina.
- `id_rotina` : Vincula o e-mail à rotina cadastrada. **(OBRIGATÓRIO)**;
- `email` : Endereço de e-mail do destinatário para recebimento da mensagem gerada pela rotina.

### **`estrutura_informativo`**: Estrutura de organização de imagens no corpo do informativo
- `codigo` : Identificador único de cada imagem. **(SEQUENCE)**;
- `id_rotina` : Vincula o e-mail à rotina cadastrada. **(OBRIGATÓRIO)**;
- `hiper_link` : Link que será vinculado à imagem dentro do corpo do informativo;
- `corpo` : Nome do arquivo de imagem com a extensão. **(OBRIGATÓRIO)**.

## ⚙️ Como Funciona

O fluxo de operação segue o ciclo abaixo:

1. **Polling:** O serviço consulta a tabela `cadastro_rotinas` em intervalos definidos.
2. **Verificação:** Filtra rotinas ativas cuja hora de execução coincide com o `now()`.
3. **Processamento:** Para cada rotina encontrada, o sistema abre uma **Thread dedicada**. Isso permite que um relatório pesado não atrase um informativo simples.
4. **Conclusão:** Após o término da tarefa, o serviço busca os contatos na `email_rotinas` e dispara um e-mail.

_Obs: Caso a rotina seja do tipo **IN** a tabela `estrutura_informativo` é consultada para verificar a organização das imagens no corpo do e-mail._

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
