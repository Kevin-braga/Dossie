import logging
logger = logging.getLogger("Listener.call.CELESC")
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
			driver.set_page_load_timeout(10)
			driver.get("http://agenciaweb.celesc.com.br:8080/AgenciaWeb/autenticar/autenticarLoginUC.do")
			WebDriverWait(driver,5).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="fundoPrincipalLogout"]/form""")))
		except (TimeoutException):
			continue

		logger.debug("GET_URL bem sucedido")
		login = True

def run(documento,tipo_documento,driver):

	from selenium.common.exceptions import NoSuchElementException , TimeoutException
	from selenium.webdriver.support.ui import WebDriverWait
	from selenium.webdriver.support import expected_conditions as EC
	from selenium.webdriver.common.keys import Keys
	from selenium.webdriver.common.by import By

	uf = 'SC'

	conn = False
	while not conn:	
		try:
			if tipo_documento == "Jurídica":
				logger.debug('Documento validado como Jurídico')

				WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="fundoPrincipalLogout"]/form/div[2]/input[1]""")))
				driver.find_element_by_xpath("""//*[@id="fundoPrincipalLogout"]/form/div[2]/input[1]""").send_keys(documento)

			elif tipo_documento == "Física":
				logger.debug('Documento validado como Físico')

				WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="fundoPrincipalLogout"]/form/div[2]/input[1]""")))
				driver.find_element_by_xpath("""//*[@id="fundoPrincipalLogout"]/form/div[2]/input[1]""").send_keys(documento[3:])

			driver.set_page_load_timeout(30)
			driver.find_element_by_xpath("""//*[@id="fundoPrincipalLogout"]/form/div[4]/input""").click()

		except (TimeoutException):
			log(driver)
			continue

		conn = True


	try:
		WebDriverWait(driver,10).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="avisoErro"]/tbody/tr/td/span[1]""")))

		logger.debug('Nada localizado')
		sql_F(documento, tipo_documento, uf)
		return
		
	except (TimeoutException):
		try:
			logger.debug('Dados encontrados')

			logradouro = driver.find_elements_by_xpath("""//*[@id="listaFat"]/span[4]""")
			unidade = driver.find_elements_by_xpath("""//*[@id="listaFat"]/span[2]""")
			
			for l,u in zip(logradouro,unidade):

				unidade = " "
				logadouro = " "
				numero = " "
				complemento = " "
				endereco_completo = " "

				endereco_completo = l.text.strip()
				unidade = u.text.strip()
				#### LOGRADOURO ####
				if "," in l.text:
					mask = l.text.split(",")
					logradouro = mask[0].strip()
				elif "-" in l.text:
					mask = l.text.split("-")
					logradouro = mask[0].strip()
				else:
					logradouro = l.text.strip()

				#### NUMERO ####	
				if "," in l.text:
					mask = l.text.split(",")
					numero = mask[1].strip()
					if "-" in numero:
						numero = numero.split("-")
						numero = numero[0].strip()
				else:
					numero = ""

				#### COMPLEMENTO ####
				if "-" in l.text:
					mask = l.text.split("-")
					if logradouro in mask[1]:
						a = mask[1].split(logradouro)
						complemento = a[1].strip()
					else:
						complemento = mask[1].strip()
				
				xml(documento,tipo_documento,uf,logradouro,numero,complemento,unidade)	
				sql_T(documento, tipo_documento, endereco_completo , logradouro, numero , complemento , uf)
			
			return

		except NoSuchElementException:
			logger.debug('Nada encontrado!?')
			sql_F(documento, tipo_documento, uf)
			return


def xml(documento,tipo_documento,uf,logradouro,numero,complemento,unidade):
	from requests import Session
	from requests.auth import HTTPBasicAuth
	import zeep
	from zeep.transports import Transport

	session = Session()
	session.auth = HTTPBasicAuth(.')
	transport_with_basic_auth = Transport(session=session)

	client = zeep.Client(
		wsdl='https://jiveasset.service-now.com/x_jam_bd_centrais_energeticas.do?WSDL',
		transport=transport_with_basic_auth
	)

	response = client.service.insert(

		u_cpf_cnpj = documento,
		u_tipo_pessoa_energeticas = tipo_documento,
		u_uf_energeticas = uf,
		u_logradouro_energeticas = logradouro,
		u_n_mero_energeticas = numero,
		u_complemento_energeticas = complemento,
		u_descri_o_centrais = "Unidade Consumidora: " + unidade
	)
	logger.debug("<Insert: Registro inserido no ServiceNow [%s]>"%documento)


def sql_T(documento, tipo_documento, endereco_completo , logradouro, numero , complemento , uf):
	import pyodbc

	if tipo_documento == "Física":
		tipo_documento = "FISICA"
	elif tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"

	try:
		banco = pyodbc.connect('.')
		cursor = banco.cursor()
		cursor.execute("insert into db_jive_2017.dbo.CENTRAIS_ENERGETICAS(DOCUMENTO_PESSOA, TIPO_PESSOA, NOME_PESSOA, ENDERECO_COMPLETO, LOGRADOURO, BAIRRO, NUMERO, COMPLEMENTO, CIDADE, UF, Data_Inclusao, Critica, FONTE_PESQUISA) values((?),(?),NULL,(?),(?),NULL,(?) ,(?) ,NULL ,(?) ,getdate(), 'Sucesso', 'CELESC')", documento, tipo_documento, endereco_completo , logradouro, numero , complemento , uf)
		banco.commit()
		cursor.close()			
		logger.debug("SQL_T bem sucedido")

	except pyodbc.Error as error:
		
		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("CELESC",error,documento)

	finally:
		banco.close()


def sql_F(documento, tipo_documento, uf):
	import pyodbc

	if tipo_documento == "Física":
		tipo_documento = "FISICA"
	elif tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"

	try:
		banco = pyodbc.connect('.')
		cursor = banco.cursor()
		cursor.execute("insert into db_jive_2017.dbo.CENTRAIS_ENERGETICAS(DOCUMENTO_PESSOA, TIPO_PESSOA, NOME_PESSOA, ENDERECO_COMPLETO, LOGRADOURO, BAIRRO, NUMERO, COMPLEMENTO, CIDADE, UF, Data_Inclusao, Critica, FONTE_PESQUISA) values((?),(?), NULL, NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,(?) ,getdate(), 'Não existe registro para este documento', 'CELESC')", documento, tipo_documento , uf)
		banco.commit()
		cursor.close()			
		logger.debug("SQL_F bem sucedido")

	except pyodbc.Error as error:
		
		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("CELESC",error,documento)

	finally:
		banco.close()