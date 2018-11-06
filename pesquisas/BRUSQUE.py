import logging
logger = logging.getLogger("Listener.call.BRUSQUE")
#######################################################################
def go(documento,tipo_documento,driver):

	log(driver)
	run(documento,tipo_documento,driver)

def log(driver):

	from selenium.common.exceptions import NoSuchElementException , UnexpectedAlertPresentException , TimeoutException
	from selenium.webdriver.support.ui import WebDriverWait
	from selenium.webdriver.support import expected_conditions as EC
	from selenium.webdriver.common.by import By

	driver.set_page_load_timeout(60)

	login = False
	while not login:
		try:
			driver.get("https://brusque.atende.net/#!/tipo/servico/valor/49/padrao/1/load/1")
		except (UnexpectedAlertPresentException):
			driver.switch_to.alert.accept()
		except (TimeoutException):
			continue

		try:
			driver.find_element_by_xpath("""//*[@id='prosseguir']""").click()
			WebDriverWait(driver,10).until(EC.visibility_of_element_located((By.XPATH,"""//*/div/div[1]/div[2]/div/span[4]/span""")))

		except (TimeoutException , NoSuchElementException):
			continue

		logger.debug('Logado')	
		login = True

def run(documento,tipo_documento,driver):

	from selenium.common.exceptions import NoSuchElementException , TimeoutException
	from selenium.webdriver.support.ui import WebDriverWait
	from selenium.webdriver.support import expected_conditions as EC
	from selenium.webdriver.common.keys import Keys
	from selenium.webdriver.common.by import By

	uf = "SC"
	cidade = "BRUSQUE"

	validado = False	
	while not validado:	
		try:
			driver.find_element_by_xpath("""//*[@name='filtro']""").click()
			driver.find_element_by_xpath("""//*[@name='filtro']/option[2]""").click()	
			driver.find_element_by_xpath("""//*[@name='campo01']""").clear()

			if tipo_documento == "Jurídica":
				logger.debug('Documento validado como Jurídico')

				driver.find_element_by_xpath("""//*[@name='campo01']""").send_keys(documento)

			elif tipo_documento == "Física":
				logger.debug('Documento validado como Físico')

				driver.find_element_by_xpath("""//*[@name='campo01']""").send_keys(documento[3:])
				
			driver.find_element_by_xpath("""//*[@title='Consultar']""").click()

		except (NoSuchElementException):
			log(driver)
			continue

		try:
			element = WebDriverWait(driver,6).until(EC.visibility_of_element_located((By.XPATH,"""//div/div[1]/div[4]/table/tbody/tr[2]/td[2]/table/tbody/tr/td[2]/span""")))
			if element.is_displayed():
				logger.debug('Dados encontrados')
				path = driver.find_elements_by_xpath("""//div/div[1]/div[4]/table/tbody/tr[2]/td[2]/table/tbody/tr/td[2]/span""")                                                  
				for l in path:
					line = l.text
					mask = line.split("- Endereço Imóvel:")
					if "Cadastro Imobiliário:" in line:
						inscricao = mask[0].lstrip("Cadastro Imobiliário:")
					if "Endereço Imóvel:" in line:
						mask_log = mask[1].split(",")
						logradouro = mask_log[0].strip()
					if "Nro:" in line:
						nro_mask = line.split(", Nro:")
						mask3 = nro_mask[1].rsplit(",")
						numero = mask3[0].strip()
						bairro = mask3[1].strip()

					xml(documento, tipo_documento, uf, cidade, inscricao, logradouro, numero, bairro)
					sql_T(documento, tipo_documento, uf, cidade, inscricao, logradouro, numero, bairro)

				return																																							  
			
		except (TimeoutException):
			logger.debug('Nada localizado')
			sql_F(uf, cidade, tipo_documento, documento)
			return

def sql_T(documento, tipo_documento, uf, cidade, inscricao, logradouro, numero, bairro):
	import pyodbc

	if tipo_documento == "Física":
		tipo_documento = "FISICA"
	elif tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"

	try:
		banco = pyodbc.connect('.')
		cursor = banco.cursor()
		cursor.execute("insert into DB_JIVE_2017.dbo.IMOVEIS(UF,CIDADE,tipo_Pessoa,Documento_PESSOA,COD_IMOVEL,INSCR_IMOBILIARIA,LOGRADOURO,NUMERO,BAIRRO,Data_Inclusao,Critica,FL_DEVEDOR_JIVE,FL_FASE_PRECIFICACAO, FONTE_PESQUISA) values((?),(?),(?), (?),NULL,(?) ,(?) ,(?) ,(?) ,getdate(), 'Dados Capturados com Sucesso' ,'N','N' , 'BRUSQUE.ATENDE')", uf, cidade, tipo_documento, documento,inscricao, logradouro, numero, bairro)
		banco.commit()
		cursor.close()			
		logger.debug("SQL_T bem sucedido")

	except pyodbc.Error as error:
		
		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("BRUSQUE",error,documento)

	finally:
		banco.close()


def sql_F(uf, cidade, tipo_documento, documento):
	import pyodbc

	if tipo_documento == "Física":
		tipo_documento = "FISICA"
	elif tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"

	try:
		banco = pyodbc.connect('.')
		cursor = banco.cursor()
		cursor.execute("insert into DB_JIVE_2017.dbo.IMOVEIS(UF,CIDADE,tipo_Pessoa,Documento_PESSOA,COD_IMOVEL,INSCR_IMOBILIARIA,LOGRADOURO,NUMERO,BAIRRO,Data_Inclusao,Critica,FL_DEVEDOR_JIVE,FL_FASE_PRECIFICACAO, FONTE_PESQUISA) values((?),(?),(?),(?),NULL ,NULL ,NULL ,NULL ,NULL ,getdate(), 'Registro não encontrado' ,'N','N', 'BRUSQUE.ATENDE')", uf, cidade, tipo_documento, documento)
		banco.commit()
		cursor.close()			
		logger.debug("SQL_F bem sucedido")

	except pyodbc.Error as error:
		
		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("BRUSQUE",error,documento)

	finally:
		banco.close()


def xml(documento,tipo_documento,uf, cidade, inscricao, logradouro, numero, bairro):
	from requests import Session
	from requests.auth import HTTPBasicAuth
	import zeep
	from zeep.transports import Transport

	session = Session()
	session.auth = HTTPBasicAuth(.')
	transport_with_basic_auth = Transport(session=session)

	client = zeep.Client(
		wsdl='https://jiveasset.service-now.com/x_jam_bd_iptu_full.do?WSDL',
		transport=transport_with_basic_auth
	)

	response = client.service.insert(

		documento_contribuinte_1 = documento,
		tipo_contribuinte = tipo_documento,
		uf = uf,
		cidade = cidade,
		numero_contribuinte = inscricao,
		nome_logradouro_imovel = logradouro,
		numero_imovel = numero,
		bairro_imovel = bairro,
		critica = "Sucesso",
		fonte_pesquisa="BRUSQUE.ATENDE",
	)
	logger.debug("<Insert: Registro inserido no ServiceNow [%s]>"%documento)


