import logging
logger = logging.getLogger("Listener.call.SPU")
################################################
def run(documento,tipo_documento, driver):

	from selenium.common.exceptions import NoSuchElementException , TimeoutException , StaleElementReferenceException
	from selenium.webdriver.support.ui import WebDriverWait
	from selenium.webdriver.support import expected_conditions as EC
	from selenium.webdriver.common.keys import Keys
	from selenium.webdriver.common.by import By 
	import time

	if tipo_documento == "Física":
		driver.get("""http://atendimentovirtual2.spu.planejamento.gov.br/Emissoes/Certidao/FRel_Certidao.asp?CPFCNPJ={0}&Municipio=0000&Parametro=CPF&Tipo=03""".format(documento[3:]))

	elif tipo_documento == "Jurídica":
		driver.get("""http://atendimentovirtual2.spu.planejamento.gov.br/Emissoes/Certidao/FRel_Certidao.asp?CPFCNPJ={0}&Municipio=0000&Parametro=CNPJ&Tipo=03""".format(documento))

	try:
		time.sleep(3)
		driver.switch_to_frame(driver.find_element_by_name("corpo"))
	except NoSuchElementException:
		print ("ERROR")

	try:
		msg = WebDriverWait(driver,4).until(EC.visibility_of_element_located((By.XPATH,"""//strong[contains(text(),'Procurar a Gerência Regional de Patrimônio da União da localização do imóvel')]""")))
		msg = msg.text
		
		if "Procurar a Gerência Regional de Patrimônio da União da localização do imóvel" in msg:
			sql_F(documento , tipo_documento)
			return

	except TimeoutException:
		pass

	try:
		msg = driver.find_element_by_xpath("""//u[contains(text(),'Certidão de Inteiro Teor do Imóvel')]""")
		msg = msg.text

		if "Certidão de Inteiro Teor do Imóvel" in msg:

			mask_rip = driver.find_element_by_xpath("""//input[contains(@name,'RIPSIAPA')]""")
			rip = (mask_rip.get_attribute("value"))
			rip = rip.split("-")
			rip = rip[0].replace(" ","")
			parse(documento,tipo_documento,driver,rip)

			return

	except NoSuchElementException:
		pass

	done = False
	while not done:
		try:

			lista = driver.find_elements_by_xpath("""//tbody/tr/td[1]/a""")

			for num in lista:

				num = num.text
				rip = num.split("-")
				rip = rip[0].replace(" ","")
				parse(documento,tipo_documento,driver,rip)
	
			next_page = driver.find_element_by_xpath("""//a[contains(text(),'Próximos')]""")

			if next_page:
				next_page.click()
				continue

		except NoSuchElementException:
			done = True

	return

def parse(documento,tipo_documento,driver,rip):

	import requests
	from lxml import html

	r = requests.get("http://atendimentovirtual2.spu.planejamento.gov.br/Emissoes/Certidao/Cert_InteiroTeor.asp?SelRIP=1&RIP={0}&Chama=Nome&Tipo=03".format(rip))
	tree = html.fromstring(r.text)

	_input_ = (tree.xpath('//input'))

	nome_propietario = numero_rip = uf = municipio = endereco = bairro = conceituacao = area_total_terreno = area_total_uniao = fracao_ideal = regime_utilizacao = ""

	for element in _input_:
		
		if element.name == "RIPSIAPA":
			numero_rip = element.value.strip()
		if element.name == "REGIME":
			regime_utilizacao = element.value.strip()
		if element.name == "NOMERESP":
			nome_propietario = element.value.strip()
		if element.name == "TIPOLOGR":
			tipo_logradouro = element.value.strip()
		if element.name == "LOGRADOURO":
			logradouro = element.value.strip()
		if element.name == "NUMEROLOGR":
			numero = element.value.strip()
		if element.name == "strEdComp":
			complemento_imovel = element.value.strip()
		if element.name == "BAIRRO":
			bairro = element.value.strip()
		if element.name == "MunicipioEditado":
			mask = element.value
			if ',' in mask:
				mask = mask.split(',')
				municipio = mask[0].strip()
				uf = mask[1].strip()

		if element.name == "CONCEITUACAO":
			conceituacao = element.value
		if element.name == "FRACAO":
			fracao_ideal = element.value
		if element.name == "AREATOTAL":
			area_total_terreno = element.value
		if element.name == "AREAUNIAO":
			area_total_uniao = element.value

	endereco = tipo_logradouro+' '+logradouro+', '+numero+' '+complemento_imovel
	
	sql_T(documento,tipo_documento,nome_propietario,numero_rip,uf,municipio,endereco,bairro,conceituacao,area_total_terreno,area_total_uniao,fracao_ideal,regime_utilizacao)
	xml (documento,tipo_documento,nome_propietario,numero_rip,uf,municipio,endereco,bairro,conceituacao,area_total_terreno)

	return

def xml(documento,tipo_documento,nome_propietario,numero_rip,uf,municipio,endereco,bairro,conceituacao,area_total_terreno):

	from requests import Session
	from requests.auth import HTTPBasicAuth
	import zeep
	from zeep.transports import Transport

	session = Session()
	session.auth = HTTPBasicAuth(.')
	transport_with_basic_auth = Transport(session=session)

	client = zeep.Client(
		wsdl='https://jiveasset.service-now.com/x_jam_bd_spu_list.do?WSDL',
		transport=transport_with_basic_auth
	)

	response = client.service.insert(

		area_total_terreno_imovel = area_total_terreno,
		u_bairro_imovel = bairro, 
		u_cidade_imovel = municipio,
		u_conceituacao_imovel = conceituacao, 
		u_cpf_cnpj = documento,
		u_endereco_imovel = endereco,
		u_nome_proprietario = nome_propietario,
		u_rip_imovel = numero_rip,
		u_tipo_pessoa = tipo_documento,
		u_uf_imovel = uf
	)

	logger.debug("<Insert: Registro inserido no ServiceNow [%s]>"%documento)


def sql_T(documento,tipo_documento,nome_propietario,numero_rip,uf,municipio,endereco,bairro,conceituacao,area_total_terreno,area_total_uniao,fracao_ideal,regime_utilizacao):

	import pyodbc

	if tipo_documento == "Física":
		tipo_documento = "FISICA"
	elif tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"

	try:
		banco = pyodbc.connect('.')
		cursor = banco.cursor()
		cursor.execute("""INSERT INTO DB_JIVE_2017.dbo.IMOVEIS_SPU(DOCUMENTO_PROPRIETARIO, TIPO_PESSOA, NOME_PROPRIETARIO, NUMERO_RIP, UF, MUNICIPIO, ENDERECO, BAIRRO, CONCEITUACAO, AREA_TOTAL_TERRENO, AREA_TOTAL_UNIAO, FRACAO_IDEAL, REGIME_UTILIZACAO, DATA_INCLUSAO, CRITICA) 
		values((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),getdate(), 'Sucesso')""", documento,tipo_documento,nome_propietario,numero_rip,uf,municipio,endereco,bairro,conceituacao,area_total_terreno,area_total_uniao,fracao_ideal,regime_utilizacao)
		banco.commit()
		cursor.close()			
		logger.debug("SQL_T bem sucedido")

	except pyodbc.Error as error:
	
		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("SPU",error,documento)

	finally:
		banco.close()
	
def sql_F(documento , tipo_documento):

	import pyodbc

	if tipo_documento == "Física":
		tipo_documento = "FISICA"
	elif tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"

	try:
		banco = pyodbc.connect('.')
		cursor = banco.cursor()
		cursor.execute("INSERT INTO DB_JIVE_2017.dbo.IMOVEIS_SPU(DOCUMENTO_PROPRIETARIO, TIPO_PESSOA, DATA_INCLUSAO, CRITICA) values((?),(?),getdate(), 'Não há imoveis para os parametros informados')", documento, tipo_documento)
		banco.commit()
		cursor.close()
		logger.debug("SQL_F bem sucedido")			

	except pyodbc.Error as error:

		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("SPU",error,documento)

	finally:
		banco.close()

if __name__ == "__main__":
	from selenium import webdriver

	path = r"C:\Users\kevin.braga\Downloads\Kevin\BACKUP\Python Scripts\chromedriver.exe"
	driver = webdriver.Chrome(path);


	run("00044785395885","Física",driver)
	print ("Fim")