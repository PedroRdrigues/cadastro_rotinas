from smtplib import SMTP_SSL
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.utils import formatdate
from email import encoders
from base64 import b64encode
from os import path, getenv
from time import sleep

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError as e:
    print("dotenv não Instalado.")


EMAIL_DEFAULT_USER = getenv("EMAIL_DEFAULT_USER")
EMAIL_DEFAULT_PASSWORD = getenv("EMAIL_DEFAULT_PASSWORD")
EMAIL_HOST = getenv("EMAIL_HOST")
EMAIL_PORT = int(getenv("EMAIL_PORT"))



class Email:
    def __init__(
            self, user:str=EMAIL_DEFAULT_USER, password:str=EMAIL_DEFAULT_PASSWORD,
            para:list=None, cco:list=None, anexos:list=None, titulo:str=None, corpo_texto:str=None, corpo_arq:list=None
        ):
        self.__HOST = EMAIL_HOST
        self.__PORT = EMAIL_PORT
        self.__USER = user
        self.__PASSWORD = password

        self.__msg = MIMEMultipart()
        self.__msg['Date'] = formatdate(localtime=True)
        self.__msg['From'] = self.__USER # 'no-reply@grupomonaco.com.br'

        self.titulo = titulo
        self.anexos = anexos
        self.corpo_texto = corpo_texto
        self.corpo_arq = corpo_arq

        if not para and not cco:
            raise Exception("Nenhum destinatário informado.")
        else:
            self.para = para
            self.cco = cco

        self.__mensagem()


    # 1. Criando a mensagem
    def __mensagem(self):
        try:
            if self.para:
                self.para = ', '.join(self.para) if len(self.para) > 1 else self.para[0]

            if self.cco:
                self.cco = ', '.join(self.cco) if len(self.cco) > 1 else self.cco[0]


            self.__msg['Subject'] = self.titulo
            self.__msg['To'] = self.para
            self.__msg['Bcc'] = self.cco


            # 1.1. Adicionando o Anexo
            if self.anexos:
                for anexo in self.anexos:
                    if path.exists(anexo):
                        with open(anexo, 'rb') as anx:
                            part = MIMEBase('application', 'octet-stream')
                            part.set_payload(anx.read())

                        encoders.encode_base64(part)
                        part.add_header('Content-Disposition', f'attachment; filename={path.basename(anexo)}')
                        self.__msg.attach(part)

            if self.corpo_texto:
                self.__msg.attach(MIMEText(self.corpo_texto, _subtype='plain'))

            elif self.corpo_arq:
                # 2. Criar o corpo HTML com a imagem vinculada (cid)
                html = f"""<html><body>"""
                for i in range(len(self.corpo_arq)):
                    html += f"""<img src="cid:image{i}" alt="Imagem {i}"><br>"""

                html += """</body></html>"""

                msg_text = MIMEText(html, 'html')
                self.__msg.attach(msg_text)

                for i, c in enumerate(self.corpo_arq):
                    # 3. Carregar a imagem e adicionar o Content-ID
                    with open(c, 'rb') as img:
                        msg_image = MIMEImage(img.read())
                        # O Content-ID precisa corresponder ao cid no HTML (image1)
                        msg_image.add_header('Content-ID', f'<image{i}>')
                        msg_image.add_header('Content-Disposition', 'inline', filename=path.basename(c))
                        self.__msg.attach(msg_image)

        except Exception as e:
            print(f"Erro ao criar a mensagem: {e}")
            raise Exception(f"Erro ao criar a mensagem: {e}")


    # 2. Enviando (Usando SMTP_SSL para a porta 465)
    def enviar(self):
        try:
            # 1. Preparar Usuário e Senha em Base64 (Strings ASCII limpas)
            user_b64 = b64encode(self.__USER.encode('utf-8')).decode('ascii')
            pass_b64 = b64encode(self.__PASSWORD.encode('utf-8')).decode('ascii')

            with SMTP_SSL(self.__HOST, self.__PORT) as server:
                # server.set_debuglevel(1)

                # 2. Iniciar o protocolo manualmente
                server.ehlo()

                # 3. Comando AUTH LOGIN: O servidor responde 334 avisando que está pronto para o usuário
                code, resp = server.docmd("AUTH", "LOGIN")
                if code != 334:
                    raise Exception(f"Servidor não suporta AUTH LOGIN: {code} {resp}")

                # 4. Envia o Usuário em Base64
                code, resp = server.docmd(user_b64)
                if code != 334:
                    raise Exception(f"Erro no envio do usuário: {code} {resp}")

                # 5. Envia a Senha em Base64
                code, resp = server.docmd(pass_b64)
                if code != 235:  # 235 é o código de sucesso para Autenticação
                    raise Exception(f"Erro na senha (provavelmente recusada): {code} {resp}")

                print("---[ Autenticado com sucesso! ]---")

                # 6. Enviar o e-mail
                server.send_message(self.__msg)
                server.quit()
                print("\n---[ E-mail enviado ]---")
                sleep(1)

        except Exception as e:
            print(f"Falha crítica no envio: {e}")
            raise Exception(f"Falha crítica no envio: {e}")

