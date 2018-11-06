from selenium.common.exceptions import NoSuchElementException , TimeoutException , ElementNotVisibleException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium import webdriver
from requests import Session
from requests.auth import HTTPBasicAuth
import logging, pyodbc, zeep
from zeep.transports import Transport
from modules import delete

logger = logging.getLogger("Listener.call.BETHA")

class Betha():

	def __init__(self,cedente):

		self.cedente = cedente
		print(cedente)
		self.query = """DECLARE @CIDADE AS VARCHAR(50) = '{1}' SELECT DOCUMENTO_PESSOA,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}') 
		AND NOT EXISTS (SELECT DOCUMENTO_PESSOA FROM DB_JIVE_2017.DBO.IMOVEIS WHERE DBO.DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = DBO.IMOVEIS.DOCUMENTO_PESSOA AND CIDADE = @CIDADE)"""
		self.driver = webdriver.Chrome("chromedriver.exe");
		self.connection = "."
		self.driver.set_page_load_timeout(15)
		
	def run_massivo(self):

		banco = pyodbc.connect(self.connection)
		cursor = banco.cursor()

		cidades = self.get_cidades()
		
		for line in cidades:

			line = line.split(';')

			self.cidade = line[0]
			self.uf = line[1]
			mainForm_estado = line[2]
			mainForm_municipio = line[3].strip()

			try:
				self.login(mainForm_estado,mainForm_municipio)
				if self.check():
					for line in cursor.execute(self.query.format(self.cedente,self.cidade)):

						self.documento = line[0]
						self.tipo_documento = line[1]
					
						self.search()
				else:
					continue
		
		
			except TimeoutException:
				
				pass

		self.driver.quit()

	def get_cidades(self):

		c = open ('cidades.csv', 'r')
		c = (c.readlines()[1:])
		return (c)
		

	def login(self,mainForm_estado,mainForm_municipio):

		login = False
		while not login:
			try:
				self.driver.get("https://e-gov.betha.com.br/cidadaoweb3/main.faces")
				button_estado = WebDriverWait(self.driver,2).until(EC.visibility_of_element_located((By.XPATH,mainForm_estado))).click()
				button_municipio = WebDriverWait(self.driver,2).until(EC.visibility_of_element_located((By.XPATH,mainForm_municipio))).click()
				self.driver.find_element_by_xpath("""//*[@id="mainForm:selecionar"]""").click()

			except TimeoutException:
				continue

			login = True


	def check(self):

		if self.driver.current_url == "https://e-gov.betha.com.br/cidadaoweb3/main.faces":
			return False

		url = self.driver.current_url
		url = url.replace('/main.faces','')
		url = url + '/rel_cndimovel.faces'

		for _ in range(3):
			self.driver.get(url)
			try:
				self.driver.find_element_by_xpath("""//h1[contains(text(),'Qual sistema você deseja acessar?')]""")
				continue
			except NoSuchElementException:
				pass
			try:
				self.driver.find_element_by_xpath("""//p[contains(text(),'Esse recurso foi desativado. Para maiores informações entre em contato com a prefeitura.')]""")
				continue
			except NoSuchElementException:
				pass

			try:
				self.driver.find_element_by_xpath("""//*[@id="mainForm:g-CPF"]""")
				pass
			except NoSuchElementException:
				continue

			return True

		return False
		

	def search(self):

		if self.tipo_documento == 'JURIDICA':
			WebDriverWait(self.driver,2).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="modoAcesso"]/div/div[2]/div/div/div/a"""))).click()
			WebDriverWait(self.driver,2).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="mainForm:cnpj"]"""))).send_keys(self.documento)
			button = self.driver.find_element_by_xpath("""//*[@id="mainForm:btCnpj"]""")
			self.driver.execute_script("arguments[0].click()", button)
		
		if self.tipo_documento == 'FISICA':
			WebDriverWait(self.driver,2).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="modoAcesso"]/div/div[1]/div/div/div/a"""))).click()
			WebDriverWait(self.driver,2).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="mainForm:cpf"]"""))).send_keys(self.documento[3:])
			button = self.driver.find_element_by_xpath("""//*[@id="mainForm:btCpf"]""")
			self.driver.execute_script("arguments[0].click()", button)

		for _ in range(11):
			try:
				WebDriverWait(self.driver,0.5).until(EC.visibility_of_element_located((By.XPATH,"""//div[@id='mainForm:master:messageSection:error']""")))
				self.sql_F(self.documento,self.tipo_documento,self.cidade,self.uf)
				return
			except TimeoutException:
				pass

			try:
				WebDriverWait(self.driver,0.5).until(EC.visibility_of_element_located((By.XPATH,"""//div[@id='mainForm:master:messageSection:warn']""")))
				self.sql_F(self.documento,self.tipo_documento,self.cidade,self.uf)
				return
			except TimeoutException:
				pass

		self.parse()

	def parse(self):

		try:
			msg = WebDriverWait(self.driver,20).until(EC.visibility_of_element_located((By.XPATH,"""//p[contains(text(),'A certidão negativa de imóvel está pronta para ser emitida. Basta clicar sobre o ícone da coluna emitir.')]""")))
		
			logger.debug('Dados encontrados')

			#coluna 1
			col_1 = self.driver.find_elements_by_xpath("""//*[@id="mainForm:t-imoveis"]/tbody/tr/td[1]""")
			#coluna 2
			col_2 = self.driver.find_elements_by_xpath("""//*[@id="mainForm:t-imoveis"]/tbody/tr/td[2]""")
			#coluna 3
			col_3 = self.driver.find_elements_by_xpath("""//*[@id="mainForm:t-imoveis"]/tbody/tr/td[3]""")
				
			for row in zip (col_1,col_2,col_3):
	
				codigo_imovel = row[0].text.strip()
				inscricao_imobiliaria = row[1].text.strip()
				endereco_completo = row[2].text.strip()

				#### LOGRADOURO ####
				try:
					mask = endereco_completo.split("- Bairro: ")
					logradouro = mask[0].split(",")
					logradouro = logradouro[0].lstrip()
				
				except IndexError:
					logradouro = " "

				#### BAIRRO ####
				try:		
					mask = endereco_completo.split("- Bairro: ")
					bairro = mask[1].split("- CEP:")
					bairro = bairro[0].lstrip()
					
				except IndexError:
					bairro = " "

				#### CEP ####
				try:
					mask = endereco_completo.split("- Bairro: ")
					cep = mask[1].split("- CEP:")
					cep = cep[1].lstrip()
				
				except IndexError:
					cep = " "

				#### NUMERO ####	
				try:
					mask = endereco_completo.split("- Bairro: ")
					numero = mask[0].split(",")
					numero = numero[1].lstrip()
					
				except IndexError:
					numero = "S/N"

				self.sql_T(self.documento,self.tipo_documento,self.cidade,self.uf,codigo_imovel,inscricao_imobiliaria,logradouro,numero,bairro)
				self.xml(self.documento,self.tipo_documento,self.cidade,self.uf,codigo_imovel,inscricao_imobiliaria,logradouro,numero,bairro,cep)

		except TimeoutException:
			logger.warn('Timeout?!')
			self.sql_F(self.documento,self.tipo_documento,self.cidade,self.uf)
			return


	def sql_T(self,documento,tipo_documento,cidade,uf,codigo_imovel,inscricao_imobiliaria,logradouro,numero,bairro):

		if tipo_documento == "Física":
			tipo_documento = "FISICA"
		elif tipo_documento == "Jurídica":
			tipo_documento = "JURIDICA"

		try:
			banco = pyodbc.connect(self.connection)
			cursor = banco.cursor()
			cursor.execute("""INSERT INTO DB_JIVE_2017.dbo.IMOVEIS(DOCUMENTO_PESSOA,TIPO_PESSOA,CIDADE,UF,COD_IMOVEL,INSCR_IMOBILIARIA,LOGRADOURO,NUMERO,BAIRRO,DATA_INCLUSAO,CRITICA,FONTE_PESQUISA) 
			values((?),(?),(?),(?),(?),(?),(?),(?),(?),getdate(),'Dados Capturados com Sucesso','BETHA')""",documento,tipo_documento,cidade,uf,codigo_imovel,inscricao_imobiliaria,logradouro,numero,bairro)
			banco.commit()
			cursor.close()			
			logger.debug("SQL_T bem sucedido")

		except pyodbc.Error as error:
		
			from modules import _email
			logger.error("<ERROR: %s [%s]>"%(error, documento))
			_email.send("BETHA",error,documento)

		finally:
			banco.close()
		

	def sql_F(self,documento,tipo_documento,cidade,uf):

		if tipo_documento == "Física":
			tipo_documento = "FISICA"
		elif tipo_documento == "Jurídica":
			tipo_documento = "JURIDICA"

		try:
			banco = pyodbc.connect(self.connection)
			cursor = banco.cursor()
			cursor.execute("""INSERT INTO DB_JIVE_2017.dbo.IMOVEIS(DOCUMENTO_PESSOA,TIPO_PESSOA,CIDADE,UF,DATA_INCLUSAO,CRITICA,FONTE_PESQUISA) 
			values((?),(?),(?),(?),getdate(),'Registro não encontrado','BETHA')""",documento,tipo_documento,cidade,uf)
			banco.commit()
			cursor.close()			
			logger.debug("SQL_F bem sucedido")

		except pyodbc.Error as error:
		
			from modules import _email
			logger.error("<ERROR: %s [%s]>"%(error, documento))
			_email.send("BETHA",error,documento)

		finally:
			banco.close()

	def xml(self,documento,tipo_documento,cidade,uf,codigo_imovel,inscricao_imobiliaria,logradouro,numero,bairro,cep):

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
			cidade = cidade,
			uf = uf,
			codigo_imovel = codigo_imovel,
			numero_contribuinte = inscricao_imobiliaria,
			nome_logradouro_imovel = logradouro,
			numero_imovel = numero,
			bairro_imovel = bairro,
			cep = cep,
			critica = "Sucesso",
			fonte_pesquisa="BETHA",
		
		)
		
		logger.debug("<Insert: Registro inserido no ServiceNow [%s]>"%documento)

if __name__ == "__main__":

	betha = Betha('00012144371920','Física')

	betha.run()