import logging
logger = logging.getLogger("Listener.call.CNDT")
########################################################################
def go(documento,tipo_documento,driver):
	log(driver)
	run(documento,tipo_documento,driver)

def log(driver):

	from selenium.webdriver.support.ui import WebDriverWait	
	from selenium.common.exceptions import TimeoutException
	from selenium.webdriver.support import expected_conditions as EC
	from selenium.webdriver.common.by import By

	login = False
	while not login:
		try:
			driver.get("http://aplicacao.jt.jus.br/cndtCertidao/gerarCertidao.faces")
		except TimeoutException:
			continue

		logger.debug("GET_URL bem sucedido")	
		login = True

def run(documento,tipo_documento,driver):

	from selenium.common.exceptions import NoSuchElementException , TimeoutException , StaleElementReferenceException
	from selenium.webdriver.support.ui import WebDriverWait
	from selenium.webdriver.support import expected_conditions as EC
	from selenium.webdriver.common.keys import Keys
	from selenium.webdriver.common.by import By 
	from modules import captcha
	import shutil , os.path , time

	if tipo_documento == "Jurídica":
		logger.debug('Documento validado como Júridico')

		driver.find_element_by_xpath("""//*[@id="gerarCertidaoForm:cpfCnpj"]""").clear()
		driver.find_element_by_xpath("""//*[@id="gerarCertidaoForm:cpfCnpj"]""").send_keys(documento)
		file = documento

	elif tipo_documento == "Física":
		logger.debug('Documento validado como Físico')

		driver.find_element_by_xpath("""//*[@id="gerarCertidaoForm:cpfCnpj"]""").clear()
		driver.find_element_by_xpath("""//*[@id="gerarCertidaoForm:cpfCnpj"]""").send_keys(documento[3:])
		file = documento[3:]


	url = driver.current_url

	webkey = "6LeKKAoUAAAAAJwv60Xf2N9-8Ri2mVJVp6dQaw6H"

	captcha_response = captcha.solve_recaptcha(url,webkey)
	logger.debug('Captcha resolvido com sucesso')

	element = driver.find_element_by_xpath("""//*[@id="g-recaptcha-response"]""")

	driver.execute_script("arguments[0].setAttribute('style','display: true;')", element)

	element.send_keys(captcha_response)

	driver.find_element_by_xpath("""//*[@id="gerarCertidaoForm:btnEmitirCertidao"]""").click()

	downloads = r"C:\Users\adm_local\Downloads\certidao_{0}.pdf".format(file)
	pdf = r"C:\files\prd\codes\arquivos\certidao_{0}.pdf".format(file)

	for _ in range(5):
		try:
			msg = driver.find_element_by_xpath("""//*[@id="mensagens"]/ul/li""")
			msg = msg.text

			if "O CNPJ / CPF informado é inválido" in msg:
				logger.debug(msg)
				sql_F(documento, tipo_documento)
				return	

		except NoSuchElementException:
			pass

		try:
			time.sleep(12)
			shutil.move(downloads, pdf)
			break	

		except FileNotFoundError:
			continue

	if os.path.exists(pdf):

		logger.debug('PDF baixado com sucesso')

		processos = read_file(pdf)

		if processos:

			logger.debug('Dados encontrados')

			for processo in processos:
						
				processo = (processo.replace("\n",""))
				mask = processo.split(" - ")
				mask1 = mask[1].split("Região")

				regiao = mask1[0].strip()
				cod_processo = mask[0].strip()

				if "**" in mask1[1]:
					execucao = "Débito com exigibilidade suspensa."
				elif "*" not in mask[1]:
					execucao = "Não está em fase de execução"
				else:
					execucao = "Débito garantido por depósito, bloqueio de numerário ou penhora de bens suficientes."

				xml(documento, tipo_documento, cod_processo, regiao, execucao)
				sql_T(documento, tipo_documento, cod_processo, regiao, execucao)	
				
		else:

			logger.debug('Dados não encontrados')
			sql_F(documento, tipo_documento)
	else:
		logger.warn('Não conseguiu baixar o PDF')
		return 
	#######

def read_file(pdf):
	import PyPDF2
	import re

	data = [] 

	with open(pdf,'rb') as file:    #'rb' for read binary mode
		pdfReader = PyPDF2.PdfFileReader(file)
		num = pdfReader.numPages

		for i in range(num):
			pageObj = pdfReader.getPage(i)          
			data.append(pageObj.extractText())

		data = " ".join(data)

		processos = []

		regular_expression = re.finditer('[0-9]{7}\-[0-9]{2}\.[0-9]{4}\.[0-9]{1}\.[0-9]{2}\.[0-9]{4} - TRT \d{2}ª\sRegião\s\**',data)
		for match in regular_expression:
			processos.append(match.group())

	return (processos)

def sql_T(documento,tipo_documento, cod_processo, regiao, execucao):
	import pyodbc

	if tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"
	elif tipo_documento == "Física":
		tipo_documento = "FISICA"

	try:
		banco = pyodbc.connect('.')
		cursor = banco.cursor()
		cursor.execute("insert into DB_JIVE_2017.dbo.CNDT(DOCUMENTO_PESSOA,TIPO_PESSOA,REGIAO,FASE_EXECUTIVA,PROCESSO,DT_INCLUSAO,CRITICA) values((?),(?),(?),(?),(?), getdate(), 'Sucesso')", documento, tipo_documento, regiao, execucao, cod_processo)
		banco.commit()
		cursor.close()			
		logger.debug("SQL_T bem sucedido")

	except pyodbc.Error as error:
		
		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("CNDT",error,documento)
	
	finally:
		banco.close()

def sql_F(documento, tipo_documento):
	import pyodbc

	if tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"
	elif tipo_documento == "Física":
		tipo_documento = "FISICA"

	try:
		banco = pyodbc.connect('.')
		cursor = banco.cursor()
		cursor.execute("insert into DB_JIVE_2017.dbo.CNDT(DOCUMENTO_PESSOA,TIPO_PESSOA,REGIAO,FASE_EXECUTIVA,PROCESSO,DT_INCLUSAO,CRITICA) values((?),(?),NULL,NULL,NULL ,getdate(), 'Não foi encontrado nenhum processo para este documento')", documento, tipo_documento)
		banco.commit()
		cursor.close()		
		logger.debug("SQL_F bem sucedido")

	except pyodbc.Error as error:
		
		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("CNDT",error,documento)

	finally:
		banco.close()


def xml(documento, tipo_documento, cod_processo, regiao, execucao):
	from requests import Session
	from requests.auth import HTTPBasicAuth
	import zeep
	from zeep.transports import Transport

	session = Session()
	session.auth = HTTPBasicAuth(.')
	transport_with_basic_auth = Transport(session=session)

	client = zeep.Client(
		wsdl='https://jiveasset.service-now.com/x_jam_bd_cndt.do?wsdl',
		transport=transport_with_basic_auth
	)

	response = client.service.insert(
		tipo_pessoa = tipo_documento,
		u_cpf_cnpj = documento,
		u_critica = "Sucesso",
		u_fase_executiva = execucao,
		u_processo_cndt = cod_processo,
		u_regiao_cndt = regiao
	)

	logger.debug("<Insert: Registro inserido no ServiceNow [%s]>"%documento)