import smtplib
from email.mime.text import MIMEText
import os

smtp_server = 'smtp.gmail.com'
myemail = os.environ('EMAIL')
smtp_port = 587



msg = MIMEText('MENSAJE')

msg['Subject'] = 'subj'
msg['From'] = myemail
msg['To'] = myemail


server = smtplib.SMTP(smtp_server, smtp_port)
server.starttls()
server.login(myemail, os.environ('PSS'))

server.send_message(msg)

server.quit()


