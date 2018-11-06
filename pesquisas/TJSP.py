import logging
logger = logging.getLogger("Listener.call.TJSP")
########################################################################
def run(documento,tipo_documento,driver):

	from selenium.common.exceptions import NoSuchElementException , TimeoutException , StaleElementReferenceException
	from selenium.webdriver.support.ui import WebDriverWait
	from selenium.webdriver.support import expected_conditions as EC
	from selenium.webdriver.common.keys import Keys
	from selenium.webdriver.common.by import By 
	from modules import captcha

	uf = 'SP'

	if tipo_documento == "Jurídica":
		logger.debug('Documento validado como Júridico (Não pesquisa)')
		sql_F(documento, tipo_documento)
		return

	elif tipo_documento == "Física":
		logger.debug('Documento validado como Físico')

	conn = False
	while not conn:
		try:
			driver.set_page_load_timeout(15)
			driver.get("""http://www.tjsp.jus.br/cac/scp/webmenupesquisa.aspx""")
			driver.find_element_by_xpath("""//*[@id="TXTITEM1"]/a""").click()
		except TimeoutException:
			continue
		conn = True

		try:
			WebDriverWait(driver,2).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="TABLE2"]//input[3]"""))).click()
			WebDriverWait(driver,2).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="vCRE_CPF_CNPJ"]"""))).clear()
			WebDriverWait(driver,2).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="vCRE_CPF_CNPJ"]"""))).send_keys(documento[3:])
		except TimeoutException:
			logger.warn("Timeout")
			return

	### QUEBRA DE CAPTCHA VAI AQUI ###
	solved = False
	while not solved:

		img = driver.find_element_by_xpath("""//*[@id="captchaImage"]/img""")
		print_screen(img,driver)
		captcha_response = captcha.solve_captcha(r'C:\files\prd\codes\arquivos\captcha.png')
		logger.debug('Captcha resolvido com sucesso')

		driver.find_element_by_xpath("""//*[@id="_cfield"]""").send_keys(captcha_response)
		driver.find_element_by_xpath("""//*[@id="TABLE8"]//td[1]/input""").click()

		try:
			error = WebDriverWait(driver,1).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="gxErrorViewer"]/span""")))
			if error.is_displayed():
				if "CPF Incorreto" in error.text:
					sql_F(documento, tipo_documento)
					solved = True

				driver.find_element_by_xpath("""//*[@id="TABLE8"]//td[1]/input""").click()
				continue
		except TimeoutException:
			solved = True

	try:
		msg = WebDriverWait(driver,10).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="TXTNENHUM"]""")))
		msg = msg.text

		if "Não foram encontrados precatórios" in msg:
			logger.debug("Nada encontrado")
			sql_F(documento, tipo_documento)
			return

	except TimeoutException:
		pass

	try:
		msg = WebDriverWait(driver,10).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="TXTEXISTE"]""")))
		logger.debug("Dados localizados")
		
		tbody = driver.find_elements_by_xpath("""//tr[contains(@id,'Grid1ContainerRow_')]""")

		for tr in tbody:

			td = tr.find_elements_by_tag_name("span")

			desc_protocolo = td[2].text
			num_processo = td[3].text
			devedor_precatorio = td[6].text
			situacao_precatorio = td[7].get_attribute('textContent')

			sql_T(documento, tipo_documento, num_processo, devedor_precatorio, situacao_precatorio, desc_protocolo, uf)
			xml (documento, tipo_documento, num_processo, devedor_precatorio, situacao_precatorio, desc_protocolo, uf)

		return

	except TimeoutException:
		logger.warn("Timeout?!")
		return


def print_screen(img,driver):
	from PIL import Image
	
	location = img.location
	size = img.size
	driver.save_screenshot(r'C:\files\prd\codes\arquivos\captcha.png')
	im = Image.open(r'C:\files\prd\codes\arquivos\captcha.png')
	left = location['x']
	top = location['y']
	right = location['x'] + size['width']
	bottom = location['y'] + size['height']
	im = im.crop((left,top,right,bottom))
	im.save(r'C:\files\prd\codes\arquivos\captcha.png')


def xml(documento, tipo_documento, num_processo, devedor_precatorio, situacao_precatorio, desc_protocolo, uf):

	from requests import Session
	from requests.auth import HTTPBasicAuth
	import zeep
	from zeep.transports import Transport

	session = Session()
	session.auth = HTTPBasicAuth(.')
	transport_with_basic_auth = Transport(session=session)

	client = zeep.Client(
		wsdl='https://jiveasset.service-now.com/x_jam_bd_precat_rios.do?WSDL',
		transport=transport_with_basic_auth
	)

	response = client.service.insert(
		u_cpf_cpj_precatorios = documento,
		u_tipo_pessoa_precatorio = tipo_documento,
		u_n_mero_processo_precatorio = num_processo,
		u_devedor_precat_rio = devedor_precatorio,	
		situa_o_precat_rio = situacao_precatorio,
		u_descri_o_protocolo_precatorio = desc_protocolo,
		u_uf_precatorio = uf,
		u_cr_tica_precatorio = "Sucesso",
		u_fonte_pesquisa_precatorio = "TJSP"
	)
	logger.debug("<Insert: Registro inserido no ServiceNow [%s]>"%documento)


def sql_T(documento, tipo_documento, num_processo, devedor_precatorio, situacao_precatorio, desc_protocolo, uf):
	import pyodbc

	if tipo_documento == "Física":
		tipo_documento = "FISICA"
	elif tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"

	try:
		banco = pyodbc.connect('.')
		cursor = banco.cursor()

		cursor.execute(""" 
		INSERT INTO DB_JIVE_2017.dbo.PRECATORIOS(
		DOCUMENTO_PESSOA,
		TIPO_PESSOA,
		NOME_PESSOA,
		NUM_PROCESSO,
		DEVEDOR_PRECATORIO,
		SITUACAO_PRECATORIO,
		DESC_PROTOCOLO,
		UF,
		DATA_INCLUSAO,
		CRITICA,
		FL_DEVEDOR_JIVE,
		FL_FASE_PRECIFICACAO,
		FONTE_PESQUISA
		) 
		VALUES ((?),(?),NULL,(?),(?),(?),(?),(?),getdate(),'Dados do Precatorio capturados com Sucesso','N','N','TJSP')""",
		documento, tipo_documento, num_processo, devedor_precatorio, situacao_precatorio, desc_protocolo, uf)
		banco.commit()
		cursor.close()  
		logger.debug("SQL_T bem sucedido")        

	except pyodbc.Error as error:

		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("TJSP",error,documento)

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
		INSERT INTO DB_JIVE_2017.dbo.PRECATORIOS(
		DOCUMENTO_PESSOA,
		TIPO_PESSOA,
		NOME_PESSOA,
		DATA_INCLUSAO,
		CRITICA,
		FL_DEVEDOR_JIVE,
		FL_FASE_PRECIFICACAO,
		FONTE_PESQUISA
		)
		VALUES ((?),(?),NULL,getdate(),'Não existem precatórios para esta pessoa','N','N','TJSP')""",documento,tipo_documento)
		banco.commit()
		cursor.close()
		logger.debug("SQL_F bem sucedido")          

	except pyodbc.Error as error:

		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("TJSP",error,documento)

	finally:
		banco.close()

