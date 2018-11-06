import logging
logger = logging.getLogger("Listener.call.ITAJAÍ")
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
			driver.get("http://iptu.itajai.sc.gov.br/")
			WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="agrupador-area"]/div[2]/div[2]/div/h3/a""")))
		except (TimeoutException):
			continue
		
		logger.debug("GET_URL bem sucedido")	
		login = True

def run(documento,tipo_documento,driver):

	from selenium.common.exceptions import NoSuchElementException , TimeoutException , StaleElementReferenceException
	from selenium.webdriver.support.ui import WebDriverWait
	from selenium.webdriver.support import expected_conditions as EC
	from selenium.webdriver.common.keys import Keys
	from selenium.webdriver.common.by import By 

	uf = 'SC'
	cidade = 'Itajaí'


	if tipo_documento == "Jurídica":
		logger.debug('Documento validado como Júridico')

		driver.find_element_by_xpath("""//*[@id="cbLogin"]/option[2]""").click()
		driver.find_element_by_xpath("""//*[@id="inscri"]""").send_keys(documento)


	elif tipo_documento == "Física":
		logger.debug('Documento validado como Físico')

		driver.find_element_by_xpath("""//*[@id="cbLogin"]/option[1]""").click()
		driver.find_element_by_xpath("""//*[@id="inscri"]""").send_keys(documento[3:])
		
	try:
		driver.find_element_by_xpath("""//*[@id="form_index_proprietario"]/table[2]/tbody/tr/td/input[1]""").click()

		msg = WebDriverWait(driver,15).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="agrupador-area"]/div[2]/div[2]/div/p""")))
		msg = msg.text

		if "CPF proprietário não encontrado." in msg:
			logger.debug(msg)
			sql_F(uf, cidade, tipo_documento, documento)
			return
		elif "CNPJ proprietário não encontrado." in msg:
			logger.debug(msg)
			sql_F(uf, cidade, tipo_documento, documento)
			return
		elif "CPF/CNPJ inválido." in msg:
			logger.debug(msg)
			sql_F(uf, cidade, tipo_documento, documento)
			return

	except (TimeoutException):

		logger.debug('Dados encontrados')

		inscricao = " "
		bairro = " "
		complemento = " "
		logradouro = " "

		try:
			imprimir = WebDriverWait(driver,2).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="Imprimir"]""")))
			if imprimir.is_displayed():

				## INSCRIÇÃO
				a = driver.find_element_by_xpath("""//*[@id="agrupador-area"]/div[2]/div[2]/div/div/table/tbody/tr[2]/td[2]/input""")
				## BAIRRO
				b = driver.find_element_by_xpath("""//*[@id="agrupador-area"]/div[2]/div[2]/div/div/table/tbody/tr[4]/td[4]/input""")
				## COMPLEMENTO
				c = driver.find_element_by_xpath("""//*[@id="agrupador-area"]/div[2]/div[2]/div/div/table/tbody/tr[4]/td[2]/input""")
				## LOGRADOURO
				d = driver.find_element_by_xpath("""//*[@id="agrupador-area"]/div[2]/div[2]/div/div/table/tbody/tr[3]/td[2]/input""")

				inscricao = (a.get_attribute("value").strip())
				bairro =  (b.get_attribute("value").strip())
				complemento = (c.get_attribute("value").strip())
				logradouro = (d.get_attribute("value").strip())	

				xml(documento, tipo_documento, uf, cidade, inscricao, logradouro, complemento, bairro)
				sql_T(documento, tipo_documento, uf, cidade, inscricao, logradouro,complemento, bairro)
				return	

		except TimeoutException:

				## INSCRIÇÃO
			a = driver.find_elements_by_xpath("""//*[@id="agrupador-area"]/div[2]/table[2]/tbody/tr/td[2]""")
				## BAIRRO
			b = driver.find_elements_by_xpath("""//*[@id="agrupador-area"]/div[2]/table[2]/tbody/tr/td[5]""")
				## COMPLEMENTO
			c = driver.find_elements_by_xpath("""//*[@id="agrupador-area"]/div[2]/table[2]/tbody/tr/td[4]""")
				## LOGRADOURO
			d = driver.find_elements_by_xpath("""//*[@id="agrupador-area"]/div[2]/table[2]/tbody/tr/td[3]""")
			
			for inscricao,bairro,complemento,logradouro in zip(a,b,c,d):
				inscricao = (inscricao.text.strip())
				bairro = (bairro.text.strip())
				complemento = (complemento.text.strip())
				logradouro = (logradouro.text.strip())

				xml(documento, tipo_documento, uf, cidade, inscricao, logradouro, complemento, bairro)
				sql_T(documento, tipo_documento, uf, cidade, inscricao, logradouro,complemento, bairro)
			
			return

def xml(documento,tipo_documento,uf, cidade, inscricao, logradouro, complemento, bairro):
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
		complemento_imovel = complemento,
		bairro_imovel = bairro,
		critica = "Sucesso",
		fonte_pesquisa="Portal cidadão Itajaí",
	)
	
	logger.debug("<Insert: Registro inserido no ServiceNow [%s]>"%documento)

def sql_T(documento, tipo_documento, uf, cidade, inscricao, logradouro,complemento, bairro):
	import pyodbc

	if tipo_documento == "Física":
		tipo_documento = "FISICA"
	elif tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"

	cidade = 'ITAJAI'

	try:	
		banco = pyodbc.connect('.')
		cursor = banco.cursor()
		cursor.execute("insert into DB_JIVE_2017.dbo.IMOVEIS(UF,CIDADE,tipo_Pessoa,Documento_PESSOA,COD_IMOVEL,INSCR_IMOBILIARIA,LOGRADOURO,NUMERO,BAIRRO,Data_Inclusao,Critica,FL_DEVEDOR_JIVE,FL_FASE_PRECIFICACAO, FONTE_PESQUISA) values((?),(?),(?),(?),NULL,(?),(?),(?),(?),getdate(),'Dados Capturados com Sucesso','N','N' , 'Portal cidadão Itajaí')", uf, cidade, tipo_documento, documento, inscricao, logradouro,complemento, bairro)
		banco.commit()
		cursor.close()			
		logger.debug("SQL_T bem sucedido")

	except pyodbc.Error as error:
		
		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("ITAJAI",error,documento)

	finally:
		banco.close()

def sql_F(uf, cidade, tipo_documento, documento):
	import pyodbc

	if tipo_documento == "Física":
		tipo_documento = "FISICA"
	elif tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"

	cidade = 'ITAJAI'

	try:
		banco = pyodbc.connect('.')
		cursor = banco.cursor()
		cursor.execute("insert into DB_JIVE_2017.dbo.IMOVEIS(UF,CIDADE,tipo_Pessoa,Documento_PESSOA,COD_IMOVEL,INSCR_IMOBILIARIA,LOGRADOURO,NUMERO,BAIRRO,Data_Inclusao,Critica,FL_DEVEDOR_JIVE,FL_FASE_PRECIFICACAO, FONTE_PESQUISA) values((?),(?),(?),(?),NULL ,NULL ,NULL ,NULL ,NULL ,getdate(), 'Registro não encontrado' ,'N','N', 'Portal cidadão Itajaí')", uf, cidade, tipo_documento, documento)
		banco.commit()
		cursor.close()			
		logger.debug("SQL_F bem sucedido")
		
	except pyodbc.Error as error:
		
		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("ITAJAI",error,documento)

	finally:
		banco.close()