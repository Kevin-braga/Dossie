import logging
logger = logging.getLogger("Listener.modules.email")

def send(script_name , error, documento):

	import time
	import smtplib
	from email.mime.text import MIMEText

	hora = time.strftime("%d/%m/%Y-%H:%M:%S")

	# define content
	recipients = ["kevin.braga@jiveinvestments.com"]
	sender = "alerta.falhas@gmail.com"

	subject = "ERRO - Execução"

	body = """ERRO : {0}
	Script: {1}  
	Horário: {2} 
	Registro : {3}""".format(error , script_name, hora, documento)

	# make up message
	msg = MIMEText(body)
	msg['Subject'] = subject
	msg['From'] = sender
	msg['To'] = ", ".join(recipients)

	# sending
	try:
		session = smtplib.SMTP("smtp.gmail.com:587")
		session.starttls()
		session.login(sender, '.')
		send_it = session.sendmail(sender, recipients, msg.as_string())
		session.quit()
	except Exception as error:
		logger.error('<ERROR: %s>'%error)


