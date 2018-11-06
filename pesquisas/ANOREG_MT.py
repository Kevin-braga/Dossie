import logging
logger = logging.getLogger("Listener.call.ANOREG_MT")
########################################################################
def go(documento,tipo_documento,driver):
	log(driver)
	run(documento,tipo_documento,driver)

def log(driver):

	from selenium.webdriver.support.ui import WebDriverWait	
	from selenium.common.exceptions import TimeoutException , WebDriverException, NoSuchElementException	
	from selenium.webdriver.support import expected_conditions as EC
	from selenium.webdriver.common.by import By
	import time
	
	login = False
	while not login:
		try:
			driver.get("https://cei-anoregmt.com.br/Sistema/")
		except TimeoutException:
			continue

		try:
			WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="informativo"]/div/div/div[1]/button/span"""))).click()
		except TimeoutException:
			pass

		try:
			time.sleep(2)
			driver.find_element_by_xpath("""//*[@id="UserName"]""").send_keys("44785395885")
			driver.find_element_by_xpath("""//*[@id="Password"]""").send_keys("36968319kk")
			driver.find_element_by_xpath("""//*[@id="btnEntrar"]""").click()
		except (NoSuchElementException , WebDriverException):
			continue

		logger.debug('Logado')
		login = True

def run(documento,tipo_documento,driver):

	from selenium.common.exceptions import NoSuchElementException , TimeoutException , StaleElementReferenceException
	from selenium.webdriver.support.ui import WebDriverWait
	from selenium.webdriver.support import expected_conditions as EC
	from selenium.webdriver.common.keys import Keys
	from selenium.webdriver.common.by import By 

	conn = False
	while not conn:
		try:
			driver.get("""https://cei-anoregmt.com.br/Sistema/Consulta/Index""")
		except TimeoutException:
			continue

		conn = True

	if tipo_documento == "Jurídica":
		logger.debug('Documento validado como Júridico')

		driver.find_element_by_xpath("""//*[@id="busca"]""").clear()
		driver.find_element_by_xpath("""//*[@id="busca"]""").send_keys(documento)

	elif tipo_documento == "Física":
		logger.debug('Documento validado como Físico')

		driver.find_element_by_xpath("""//*[@id="busca"]""").clear()
		driver.find_element_by_xpath("""//*[@id="busca"]""").send_keys(documento[3:])

	driver.find_element_by_xpath("""//*[@id="btnEnviar"]""").click()
	###  ###
	try:
		msg = WebDriverWait(driver,60).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="dvServicos"]//h3""")))
		msg = msg.text

	except TimeoutException:
		sql_F(documento, tipo_documento)
		return

	if "Nenhum Resultado encontrado" in msg:
		logger.debug('Dados não encontrados')
		sql_F(documento, tipo_documento)
		return

	elif "Não foi possível realizar a consulta, erro interno no site" in msg:
		logger.warn('Erro interno')
		return

	elif "Encontrados os seguintes serviços" in msg:
		logger.debug('Dados encontrados')

		a = driver.find_elements_by_xpath("""//*[@id="cartorio"]/tbody/tr/td[1]""")
		#
		b = driver.find_elements_by_xpath("""//*[@id="cartorio"]/tbody/tr/td[2]""")
		#
		c = driver.find_elements_by_xpath("""//*[@id="cartorio"]/tbody/tr/td[6]""")

		for certidao, nome, total in zip(a,b,c):

			certidao = (certidao.text)
			nome = (nome.text)
			total = (total.text)

			sql_T(documento, tipo_documento, certidao, nome, total)
			xml(documento, tipo_documento, certidao, total)

def sql_T(documento, tipo_documento, certidao, nome, total):
	import pyodbc

	if tipo_documento == "Física":
		tipo_documento = "FISICA"
	elif tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"

	try:
		banco = pyodbc.connect('.')
		cursor = banco.cursor()
		cursor.execute("insert into DB_JIVE_2017.dbo.CERTIDOES_ANOREG(DOCUMENTO_PESSOA,TIPO_PESSOA,TOTAL,NOME_CERTIDAO, NOME_PESSOA,Data_Inclusao,Critica,FL_DEVEDOR_JIVE,FL_FASE_PRECIFICACAO) values((?),(?),(?),(?),(?),getdate(), 'Dados Capturados com Sucesso' ,'N','N')", documento, tipo_documento, total, certidao, nome)
		banco.commit()
		cursor.close()			
		logger.debug("SQL_T bem sucedido")

	except pyodbc.Error as error:
		
		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("ANOREG_MT",error,documento)

	finally:
		banco.close()

def sql_F(documento, tipo_documento):
	import pyodbc

	if tipo_documento == "Física":
		tipo_documento = "FISICA"
	elif tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"

	try:
		banco = pyodbc.connect('.')
		cursor = banco.cursor()
		cursor.execute("insert into DB_JIVE_2017.dbo.CERTIDOES_ANOREG(DOCUMENTO_PESSOA,TIPO_PESSOA,TOTAL,NOME_CERTIDAO,Data_Inclusao,Critica,FL_DEVEDOR_JIVE,FL_FASE_PRECIFICACAO) values((?),(?),NULL,NULL,getdate(), 'Registro não encontrado' ,'N','N')", documento, tipo_documento)
		banco.commit()
		cursor.close()			
		logger.debug("SQL_F bem sucedido")

	except pyodbc.Error as error:
		
		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("ANOREG_MT",error,documento)

	finally:
		banco.close()
		

def xml(documento, tipo_documento, certidao, total):

	from requests import Session
	from requests.auth import HTTPBasicAuth
	import zeep
	from zeep.transports import Transport

	session = Session()
	session.auth = HTTPBasicAuth(.')
	transport_with_basic_auth = Transport(session=session)

	client = zeep.Client(
		wsdl='https://jiveasset.service-now.com/x_jam_bd_certid_es_anoreg_mt.do?WSDL',
		transport=transport_with_basic_auth
	)

	response = client.service.insert(
		u_cpf_cnpj = documento,
		u_nome_certid_o_anoreg_mt = certidao ,
		u_tipo_pessoa_anoreg = tipo_documento,
		u_total_anoreg_mt = total,
		uf = "MT"
	)
	
	logger.debug("<Insert: Registro inserido no ServiceNow [%s]>"%documento)