import smtplib
from email.mime.text import MIMEText
import os
import datetime
from unidecode import unidecode
from datetime import datetime
import pytz
import os
import time


class SendEmail:
    def __init__(self, data, email, pss) -> None:
        self.DATE = self.getDate()
        self.smtp_server ='smtp.gmail.com'
        self.smtp_port = 587
        self.email = email
        self.pss = pss
        self.data = data

    def getDate(self) -> str:
        argentina_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        utc_now = datetime.utcnow()
        argentina_now = utc_now.replace(tzinfo=pytz.utc).astimezone(argentina_timezone)
        formatted_date = argentina_now.strftime('%Y-%m-%d %H:%M:%S')
        return formatted_date

    def run(self):
        mensaje = "Últimos importes por país:\n"
        data_order = sorted(self.data, key=lambda x: x['importe'], reverse=True)
        for d in data_order:
            pais = d['pais']
            ultimo_importe = '{:,}'.format(d['importe'])
            variacion = '% ' + str(d['variacion'])
            mensaje += f"Pais: {pais}, Último Importe: {ultimo_importe}, Variacion: {variacion}\n"


        msg = MIMEText(mensaje)

        msg['Subject'] = f'Precios en {self.DATE}'
        msg['From'] = self.email
        msg['To'] = self.email

        server = smtplib.SMTP(self.smtp_server, self.smtp_port)

        server.starttls()
        server.login(self.email, self.pss)

        server.send_message(msg)

        server.quit()

