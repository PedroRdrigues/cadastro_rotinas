"""
Manipulação de rotinas utilizando o terminal.

Comandos básicos CADASTRO:
Lista de comandos utilizados para manipular o cadastro da rotina.
    add = Cadastrar uma nova rotina. Ao finalizar ele retorna o id da rotina
        nome = Nome da rotina
        periodo = Periodo da rotina: H -> HORA, D-> DIARIO / M -> MENSAL  / U -> UNICO
        intervalo = Intervalo de rotina
        dta_inicial = Inicial da rotina
        dta_final = Final da rotina
        consulta = Consulta sql da rotina (Identificar se é uma consulta escrita ou o caminho de um arquivo SQL)
        tipo: Tipo de rotina:  RE -> RELATORIO com anexo / IN -> INFORMATIVO / TRG -> TRIGGER
        ativo: Se a rotina começa ativada ou não (O padrão será 'S')
    edit = Editar rotina. Passar o id da rotina como parametro. Permitir editar todos os parametros do comando "add"
    activate = Ativa uma rotina desativada. Passar o id da rotina como parametro
    deactivate = desativa uma rotina ativada. Passar o id da rotina como parametro
    search: Consultar uma ou mais rotinas. Passar o id da rotina como parametro ou "list-all" para listar todas
        {futuramente poderá ser passada uma lista de ids para consultar rotinas especificas}

Comandos básicos EMAIL:
Lista de comandos utilizado para manipular os e-mails vinculados a uma rotina.
    add: Adicioa uma lista de emails vinculados a uma rotina. Passar o id da rotina como parametro
    edit: Editar um e-mail. Passar o id da rotina e o id do e-mail como parametro
    delete: Deletar um e-mail. . Passar o id da rotina e o id do e-mail como parametros
    search: Consultar uma lista de e-mails. Passar o id da rotina como parametro

Comandos básicos INFORMATIVOS:
Lista de comandos utilizados para manipular os anexos e corpo do e-mail quando for um informativo.
    add = Adicionar um item (arquivo). Passar o id da rotina como parametro
        corpo = Caminho do arquivo do corpo do e-mail
        hiperlink = Link vinculado a uma imagem no corpo do e-mail
        anexo = Caminho do arquivo do anexo do e-mail
    edit: Edita um item. Passar o id da rotina e o código do item como parametros. Permitir editar todos os parametros do comando "add"
    delete: Deletar um item. . Passar o id da rotina e o código do item como parametros
    search: Consultar todos os itens vinculados a uma rotina. Passar o id da rotina como parametro
"""



