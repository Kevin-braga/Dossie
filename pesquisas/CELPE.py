import logging
logger = logging.getLogger("Listener.call.CELPE")
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
			driver.get("http://autoatendimento.celpe.com.br/NDP_DCSRUCES_D~home~neologw~sap.com/CDD.html?canal=hotsite&amp")
			WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="formSearch"]/table/tbody/tr[2]/td/table/tbody/tr/td[2]/button/span[2]""")))
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
	from modules import captcha

	uf = 'PE'

	try:
		if tipo_documento == "Jurídica":
			logger.debug('Documento validado como Júridico')

			driver.find_element_by_xpath("""//*[@id="formSearch"]/table/tbody/tr[1]/td[1]/li/ul[3]/div/ins""").click()
			WebDriverWait(driver,2).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="cnpj"]""")))
			driver.find_element_by_xpath("""//*[@id="cnpj"]""").clear()
			driver.find_element_by_xpath("""//*[@id="cnpj"]""").send_keys(documento)

		elif tipo_documento == "Física":
			logger.debug('Documento validado como Físico')

			driver.find_element_by_xpath("""//*[@id="formSearch"]/table/tbody/tr[1]/td[1]/li/ul[2]/div/ins""").click()
			WebDriverWait(driver,2).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="cpf"]""")))
			driver.find_element_by_xpath("""//*[@id="cpf"]""").clear()
			driver.find_element_by_xpath("""//*[@id="cpf"]""").send_keys(documento[3:])

	except:
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		return

	### QUEBRA DE CAPTCHA VAI AQUI ###
	img = driver.find_element_by_xpath("""//*[@id="kaptchaImage"]""")
	print_screen(img,driver)
	captcha_response = captcha.solve_captcha(r'C:\files\prd\codes\arquivos\captcha.png')
	logger.debug('Captcha resolvido com sucesso')

	#### ENVIAR CAPTCHA / CLICAR BOTAO PESQUISA  ###

	driver.find_element_by_xpath("""//*[@id="kaptchaAnswer"]""").send_keys(captcha_response)
	driver.find_element_by_xpath("""//*[@id="formSearch"]/table/tbody/tr[2]/td/table/tbody/tr/td[2]/button/span[2]""").click()

	###############
	try:
		invalido = WebDriverWait(driver,1).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="formSearch"]/table/tbody/tr[1]/td[2]/span[contains(@class,'help-block form-error')]""")))
		invalido = invalido.text
		
		if "CPF Inválido" in invalido:
			logger.debug('CPF Invalido')
			sql_F(documento, tipo_documento, uf )
			return

		if "CNPJ Inválido" in invalido:
			logger.debug('CNPJ Invalido')
			sql_F(documento, tipo_documento, uf )
			return

	except TimeoutException:
		pass
		
	### EXTRAÇÃO DOS DADOS ###

	ok = False
	while not ok:
		try:
			msg = driver.find_element_by_xpath("""//*[@id="2500"]/div/span""")
			
			if msg.is_displayed():

				error = msg.text
				msg.click()
				if "CPF/CNPJ Inválido" in error:
					logger.debug(error)
					sql_F(documento, tipo_documento, uf )
					return

				if "Não existe Conta Contrato associada ao cliente informado" in error:
					logger.debug(error)
					sql_F(documento, tipo_documento, uf )
					return

				if "Operação não permitida para conta contrato coletiva" in error:
					logger.debug(error)
					sql_F(documento, tipo_documento, uf )
					return

				if "Erro na validação dos dados. Aguarde alguns instantes e tente novamente" in error:
					logger.debug(error)
					sql_F(documento, tipo_documento, uf )
					return

				if "Cliente não possui faturas em aberto" in error:
					logger.debug(error)
					sql_F(documento, tipo_documento, uf )
					return

				if "Captcha digitado não confere/expirado" in error:
					logger.debug(error)
					img = driver.find_element_by_xpath("""//*[@id="kaptchaImage"]""")
					print_screen(img,driver)
					captcha_response = captcha.solve_captcha(r'C:\files\prd\codes\arquivos\captcha.png')
					driver.find_element_by_xpath("""//*[@id="kaptchaAnswer"]""").send_keys(captcha_response)
					driver.find_element_by_xpath("""//*[@id="formSearch"]/table/tbody/tr[2]/td/table/tbody/tr/td[2]/button/span[2]""").click()
					logger.debug('Captcha resolvido com sucesso')
					continue

				if "Nenhum registro encontrado para o CPF" in error:
					logger.debug(error)
					sql_F(documento, tipo_documento, uf )
					return

				if "Nenhum registro encontrado para o CNPJ" in error:
					logger.debug(error)
					sql_F(documento, tipo_documento, uf )
					return

				if "Documento validado com sucesso" in error:
					logger.debug(error)
					ok = True
					pass

		except (NoSuchElementException):
			pass

		except (StaleElementReferenceException):
			pass

	for _ in range(3):
		try:
			WebDriverWait(driver,20).until(EC.visibility_of_element_located((By.XPATH,"""//*/td[contains(@aria-describedby,'lVisaoDetalhada_endinst')]""")))
			endereco = driver.find_elements_by_xpath("""//*/td[contains(@aria-describedby,'lVisaoDetalhada_endinst')]""")
			contrato = driver.find_elements_by_xpath("""//*/td[contains(@aria-describedby,'lVisaoDetalhada_ctacontrato')]""")
			logger.debug('Dados encontrados')
			break
		except TimeoutException:
			continue
	
	check = []

	for line,cont in zip(endereco,contrato):

		line = line.text
		cont = cont.text

		if line.isspace() and cont.isspace() :
			continue

		elif line in check:
			continue

		else:

			mask = line.split(" - ")
			num = (len(mask))

			desc = cont.strip()

			logradouro = mask[0].rsplit(",")
			numero = mask[0].split(",")
			cidade = mask[num-2].strip()
			bairro = mask[num-3].strip()

			logradouro = (logradouro[0].strip())
			numero =  (numero[1].strip())
			bairro = (bairro.strip())
			cidade = (cidade.strip())

			xml(documento,tipo_documento,uf,desc,logradouro,numero,bairro,cidade)
			sql_T(documento, tipo_documento, logradouro, bairro, numero, cidade , uf)

		check.append(line)
		
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

def sql_T(documento, tipo_documento, logradouro, bairro, numero, cidade , uf):
	import pyodbc

	if tipo_documento == "Física":
		tipo_documento = "FISICA"
	elif tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"

	try:
		banco = pyodbc.connect('.')
		cursor = banco.cursor()
		cursor.execute("insert into db_jive_2017.dbo.CENTRAIS_ENERGETICAS(Documento_PESSOA, tipo_Pessoa, NOME_PESSOA, ENDERECO_COMPLETO, LOGRADOURO, BAIRRO, NUMERO, COMPLEMENTO, CIDADE, UF, Data_Inclusao, Critica, FONTE_PESQUISA) values((?),(?),NULL,NULL,(?),(?),(?) ,NULL ,(?) ,(?) ,getdate(), 'Sucesso', 'CELPE')", documento, tipo_documento, logradouro, bairro, numero, cidade , uf)
		banco.commit()
		cursor.close()			
		logger.debug("SQL_T bem sucedido")

	except pyodbc.Error as error:

		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("CELPE",error,documento)

	finally:
		banco.close()


def sql_F(documento, tipo_documento, uf ):
	import pyodbc

	if tipo_documento == "Física":
		tipo_documento = "FISICA"
	elif tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"

	try:
		banco = pyodbc.connect('.')
		cursor = banco.cursor()
		cursor.execute("insert into db_jive_2017.dbo.CENTRAIS_ENERGETICAS(Documento_PESSOA, tipo_Pessoa, NOME_PESSOA, ENDERECO_COMPLETO, LOGRADOURO, BAIRRO, NUMERO, COMPLEMENTO, CIDADE, UF, Data_Inclusao, Critica, FONTE_PESQUISA) values((?),(?), NULL, NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,(?) ,getdate(), 'Não existe registro para este documento', 'CELPE')", documento, tipo_documento , uf)
		banco.commit()
		cursor.close()			
		logger.debug("SQL_F bem sucedido")

	except pyodbc.Error as error:

		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("CELPE",error,documento)

	finally:
		banco.close()


def xml(documento,tipo_documento,uf,desc,logradouro,numero,bairro,cidade):
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
		u_descri_o_centrais = "Conta Contrato: "+ desc,
		u_logradouro_energeticas = logradouro,
		u_n_mero_energeticas = numero,
		u_bairro_energeticas = bairro,
		u_cidade_energeticas = cidade,
	)
	logger.debug("<Insert: Registro inserido no ServiceNow [%s]>"%documento)