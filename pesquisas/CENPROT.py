import logging
logger = logging.getLogger("Listener.call.CENPROT")
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
			driver.get("https://www.protestosp.com.br/consulta/index")
		except TimeoutException:
			continue

		login = True

	logger.debug("Acessado")

def run(documento,tipo_documento,driver):

	from selenium.common.exceptions import NoSuchElementException , ElementNotVisibleException , TimeoutException, StaleElementReferenceException, WebDriverException
	from selenium.webdriver.support.ui import WebDriverWait
	from selenium.webdriver.support import expected_conditions as EC
	from selenium.webdriver.common.keys import Keys
	from selenium.webdriver.common.by import By 
	from modules import captcha
	import time

	driver.find_element_by_xpath("""//*[@id="AbrangenciaNacional"]""").click()

	if tipo_documento == "Jurídica":
		driver.find_element_by_xpath("""//*[@id="TipoDocumento"]/option[3]""").click()
		driver.find_element_by_xpath("""//*[@id="Documento"]""").clear()
		driver.find_element_by_xpath("""//*[@id="Documento"]""").send_keys(documento)

	elif tipo_documento == "Física":
		driver.find_element_by_xpath("""//*[@id="TipoDocumento"]/option[2]""").click()
		driver.find_element_by_xpath("""//*[@id="Documento"]""").clear()
		driver.find_element_by_xpath("""//*[@id="Documento"]""").send_keys(documento[3:])


	url = driver.current_url
	webkey = "6Le5VP8SAAAAAGo2rBIKK7cUIIl2vQDDVZ3EEBWC"

	captcha_response = captcha.solve_recaptcha(url,webkey)

	element = driver.find_element_by_xpath("""//*[@id="g-recaptcha-response"]""")

	driver.execute_script("arguments[0].setAttribute('style','display: true;')", element)

	element.send_keys(captcha_response)

	button = driver.find_element_by_xpath("""//*[@id="frmConsulta"]/input[3]""")
	driver.execute_script("arguments[0].click()", button)

	try:
		error = WebDriverWait(driver,1).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="modal-erro-cenprot"]""")))
		if error.is_displayed():
			print ("ERRO")
			sql_F(documento,tipo_documento)
			return

	except TimeoutException:
		pass



	for _ in range(100):
		try:
			span_sp = driver.find_element_by_xpath("""//span[contains(@class,'labelTotalSP')]""").text
			span_outros = driver.find_element_by_xpath("""//span[contains(@class,'labelTotalOutros')]""").text
			driver.find_element_by_xpath("""//*[@id="modal-erro-cenprot"]//button[contains(text(),'Fechar')]""").click()

		except (StaleElementReferenceException):
			continue

		except (NoSuchElementException):
			time.sleep(1)
			continue

		except(ElementNotVisibleException):
			pass
	
		if (span_sp == "Não constam protestos") and (span_outros == "Não constam protestos"):
			sql_F(documento,tipo_documento)
			return

		if (span_sp == "Pesquisando protestos") or (span_outros == "Pesquisando protestos"):
			time.sleep(1)
			continue

		if (span_sp == "Constam protestos") or (span_outros == "Constam protestos"):
			break
		
	parse(documento,tipo_documento,driver)

########################################################################
def parse(documento,tipo_documento,driver):

	from selenium.common.exceptions import UnexpectedAlertPresentException, TimeoutException
	from selenium.webdriver.support.ui import WebDriverWait
	from selenium.webdriver.support import expected_conditions as EC
	from selenium.webdriver.common.keys import Keys
	from selenium.webdriver.common.by import By 
	import math , time

	#### PEGA O TOTAL DE PÁGINAS/RESULTADOS ####

	try:
		driver.get("https://www.protestosp.com.br/Consulta/ObterTotaisPesquisa")
		count = driver.find_element_by_xpath("//body").text.split(';')
		total = int(count[0])+int(count[1])
		pages = math.ceil(int(total)/10)

	except UnexpectedAlertPresentException:
		driver.switch_to_alert().accept()

	##### ITERA PELAS PÁGINAS ######

	for page in range(pages):

		try:
			driver.get("https://www.protestosp.com.br/Consulta/Resultados?page={0}".format(str(page+1)))
			WebDriverWait(driver, 3).until(EC.alert_is_present())
			alert = driver.switch_to_alert()
			alert.accept()

		except TimeoutException:
			pass
			
		a = driver.find_elements_by_xpath("""//td[contains(@class,'thEstado')][1]""")
		b = driver.find_elements_by_xpath("""//td[contains(@class,'thEstado')][2]""")
		c = driver.find_elements_by_xpath("""//td[contains(@class,'thCartorio')]""")

		for uf,comarca,cartorio in zip(a,b,c):
				
			uf = uf.text.strip()
			comarca = comarca.text.strip()
			cartorio = cartorio.text.strip()

			sql_T(documento, tipo_documento, comarca, cartorio, uf)
			xml(documento, tipo_documento, comarca, cartorio, uf)

##########################################################################
def xml(documento, tipo_documento, comarca, cartorio, uf):
	from requests import Session
	from requests.auth import HTTPBasicAuth
	import zeep
	from zeep.transports import Transport

	session = Session()
	session.auth = HTTPBasicAuth(.')
	transport_with_basic_auth = Transport(session=session)

	client = zeep.Client(
		wsdl='https://jiveasset.service-now.com/x_jam_bd_cenprot_list.do?WSDL',
		transport=transport_with_basic_auth
	)

	response = client.service.insert(
		u_cpf_cnpj = documento,
		u_tipo_pessoa = tipo_documento,
		uf = uf,
		u_cartorio_cenprot = cartorio,
		u_comarca = comarca,
		u_critica_cenprot = "Sucesso"
	)

	logger.debug("<Insert: Registro inserido no ServiceNow [%s]>"%documento)


def sql_T(documento, tipo_documento, comarca, cartorio, uf):
	import pyodbc

	if tipo_documento == "Física":
		tipo_documento = "FISICA"
	elif tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"

	try:
		banco = pyodbc.connect('.')
		cursor = banco.cursor()

		cursor.execute("""
		INSERT INTO DB_JIVE_2017.dbo.CENPROT(
		DOCUMENTO_PESSOA,
		TIPO_PESSOA,
		COMARCA,
		CARTORIO,
		UF,
		DATA_INCLUSAO,
		CRITICA
		)
		VALUES ((?),(?),(?),(?),(?),getdate(),'Não existem precatórios para esta pessoa')""",documento,tipo_documento,comarca,cartorio,uf)

		banco.commit()
		cursor.close()          
		logger.debug("SQL_T bem sucedido")

	except pyodbc.Error as error:

		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("CENPROT",error,documento)

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

		cursor.execute("""
		INSERT INTO DB_JIVE_2017.dbo.CENPROT(
		DOCUMENTO_PESSOA,
		TIPO_PESSOA,
		DATA_INCLUSAO,
		CRITICA
		)
		VALUES ((?),(?),getdate(),'Não existem precatórios para esta pessoa')""",documento,tipo_documento)

		banco.commit()
		cursor.close()          
		logger.debug("SQL_F bem sucedido")

	except pyodbc.Error as error:

		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("CENPROT",error,documento)

	finally:
		banco.close()
