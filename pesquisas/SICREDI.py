import logging
logger = logging.getLogger("Listener.call.SICREDI")
########################################################################
def run(documento,tipo_documento,driver):
	
	from selenium.common.exceptions import NoSuchElementException , TimeoutException , StaleElementReferenceException
	from selenium.webdriver.support.ui import WebDriverWait
	from selenium.webdriver.support import expected_conditions as EC
	from selenium.webdriver.common.keys import Keys
	from selenium.webdriver.common.by import By 

	if tipo_documento == "Jurídica":
		logger.debug('Documento validado como Júridico')

	elif tipo_documento == "Física":
		logger.debug('Documento validado como Físico (Não pesquisa)')
		sql_F(documento, tipo_documento, "-")
		return

	url = "https://ibpj.sicredi.com.br/ib-view/testarEntrada.html?cnpj={0}".format(documento)
	
	login = False
	while not login:
		try:
			driver.get(url)
		except TimeoutException:
			continue
		
		logger.debug("GET_URL bem sucedido")
		login = True

	try:
		erro = WebDriverWait(driver,12).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="errodiv"]""")))
		logger.debug('Dados não encontrados')
		sql_F(documento, tipo_documento, url)
		return
		
	except TimeoutException:
		logger.debug('Dados encontrados')
		sql_T(documento, tipo_documento, url)
		xml(documento, tipo_documento, url)
		return

def sql_T(documento, tipo_documento, url):
	import pyodbc

	if tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"

	try:
		banco = pyodbc.connect('.')
		cursor = banco.cursor()
		cursor.execute("insert into db_jive_2017.dbo.COOPERATIVA_SICREDI(DOCUMENTO_PESSOA,TIPO_PESSOA, LINK_PESQUISA, DATA_INCLUSAO, CRITICA) values((?),(?),(?) ,getdate(), 'Sucesso')", documento, tipo_documento, url)
		banco.commit()
		cursor.close()			
		logger.debug("SQL_T bem sucedido")

	except pyodbc.Error as error:
		
		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("SICREDI",error,documento)

	finally:
		banco.close()


def sql_F(documento, tipo_documento, url):
	import pyodbc

	if tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"

	try:
		banco = pyodbc.connect('.')
		cursor = banco.cursor()
		cursor.execute("insert into db_jive_2017.dbo.COOPERATIVA_SICREDI(DOCUMENTO_PESSOA,TIPO_PESSOA, LINK_PESQUISA, DATA_INCLUSAO, CRITICA) values((?),(?),(?) ,getdate(), 'Não Encontrado')", documento, tipo_documento, url)
		banco.commit()
		cursor.close()			
		logger.debug("SQL_F bem sucedido")

	except pyodbc.Error as error:
		
		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("SICREDI",error,documento)

	finally:
		banco.close()


def xml(documento, tipo_documento, url):
	from requests import Session
	from requests.auth import HTTPBasicAuth
	import zeep
	from zeep.transports import Transport

	session = Session()
	session.auth = HTTPBasicAuth(.')
	transport_with_basic_auth = Transport(session=session)

	client = zeep.Client(
		wsdl='https://jiveasset.service-now.com/x_jam_bd_sicredi_list.do?wsdl',
		transport=transport_with_basic_auth
	)

	response = client.service.insert(
		u_cpf_cnpj = documento,
		u_tipo_pessoa = tipo_documento,
		link_sicredi = url
	)

	logger.debug("<Insert: Registro inserido no ServiceNow [%s]>"%documento)