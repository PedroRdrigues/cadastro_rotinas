"""Módulo de envio de e-mails via SMTP_SSL com suporte a imagens inline (cid)."""

import logging
import smtplib
from base64 import b64encode
from email import encoders
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
from os import getenv
from pathlib import Path
from time import sleep
from typing import List, Optional, Any

# Carregamento de variáveis de ambiente
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    logging.warning("dotenv não instalado. Usando variáveis de ambiente do sistema.")


class Email:
    def __init__(
        self,
        user: str = getenv("EMAIL_DEFAULT_USER"),
        password: str = getenv("EMAIL_DEFAULT_PASSWORD"),
        para: Optional[List[str]] = None,
        cco: Optional[List[str]] = None,
        anexos: Optional[List[str]] = None,
        titulo: str = "Sem Assunto",
        corpo_texto: Optional[str] = None,
        corpo_arq: Optional[List[str]] = None,
        hiperlink: Optional[dict[str, Any]] = None
    ):
        self._host = getenv("EMAIL_HOST")
        self._port = int(getenv("EMAIL_PORT") or 465)
        self._user = user
        self._password = password

        if not para and not cco:
            raise ValueError("É necessário informar ao menos um destinatário (para ou cco).")

        self.para = para or []
        self.cco = cco or []
        self.titulo = titulo
        self.anexos = anexos or []
        self.corpo_texto = corpo_texto
        self.corpo_arq = corpo_arq or []
        self.hiperlink = hiperlink or {}

        # Objeto da mensagem
        self.msg = MIMEMultipart()
        self._montar_cabecalho()
        self._montar_corpo()

    def _montar_cabecalho(self):
        """Preenche os metadados do e-mail."""
        self.msg['Date'] = formatdate(localtime=True)
        self.msg['From'] = self._user
        self.msg['Subject'] = self.titulo
        self.msg['To'] = ", ".join(self.para)
        if self.cco:
            self.msg['Bcc'] = ", ".join(self.cco)

    def _montar_corpo(self):
        """Processa anexos, textos e imagens inline (informativos)."""
        try:
            # 1. Anexos de Arquivos (Excel, etc)
            for caminho in self.anexos:
                path_anexo = Path(caminho)
                if path_anexo.exists():
                    with open(path_anexo, 'rb') as f:
                        part = MIMEBase('application', 'octet-stream')
                        part.set_payload(f.read())
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition', f'attachment; filename={path_anexo.name}')
                    self.msg.attach(part)

            # 2. Corpo de Texto Simples
            if self.corpo_texto:
                self.msg.attach(MIMEText(self.corpo_texto, 'plain'))

            # 3. Informativos (Imagens Inline)
            elif self.corpo_arq:
                print('corpo_arq',self.corpo_arq)
                html = "<html><body>"

                for i, path_img in enumerate(self.corpo_arq):
                    link = self.hiperlink[Path(path_img).name]
                    if link:
                        html += f'<a href={link}><img src="cid:image{i}" alt="Imagem {i}"></a><br>'
                    else:
                        html += f'<img src="cid:image{i}" alt="Imagem {i}"><br>'

                html += "</body></html>"
                self.msg.attach(MIMEText(html, 'html'))
                for i, img_path in enumerate(self.corpo_arq):
                    path_img = Path(img_path)
                    if path_img.exists():
                        with open(path_img, 'rb') as f:
                            mime_img = MIMEImage(f.read())
                            mime_img.add_header('Content-ID', f'<image{i}>')
                            mime_img.add_header('Content-Disposition', 'inline', filename=path_img.name)
                            self.msg.attach(mime_img)

        except Exception as e:
            logging.error(f"Erro ao montar estrutura do e-mail: {e}")
            raise

    def enviar(self) -> bool:
        """Realiza a autenticação manual e envia o e-mail."""
        try:
            # Codificação para AUTH LOGIN
            user_b64 = b64encode(self._user.encode('utf-8')).decode('ascii')
            pass_b64 = b64encode(self._password.encode('utf-8')).decode('ascii')

            with smtplib.SMTP_SSL(self._host, self._port) as server:
                server.ehlo()

                # Autenticação manual via comandos SMTP
                code, resp = server.docmd("AUTH", "LOGIN")
                if code != 334:
                    raise PermissionError(f"Servidor recusou AUTH LOGIN: {resp}")

                server.docmd(user_b64)
                code, resp = server.docmd(pass_b64)

                if code != 235:
                    raise PermissionError(f"Autenticação recusada: {resp}")

                logging.info(f"---[ Autenticado com sucesso ]---")
                server.send_message(self.msg)

            logging.info(f"E-mail '{self.titulo}' enviado para os destinatarios")
            sleep(1)
            return True

        except Exception as e:
            logging.error(f"Falha crítica no envio de e-mail: {e}")
            return False