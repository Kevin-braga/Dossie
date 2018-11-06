import logging, requests
from requests import Session
from bs4 import BeautifulSoup
from fuzzywuzzy import fuzz

logger = logging.getLogger("Listener.call.OPENCORPORATES")

class Opencorporates():

	def __init__(self,d,n):

		self.documento = d
		self.nome = n

	def run(self):

		mask_name = self.nome.split(' ')

		#	coletando nome,sobrenome,meionome

		first_name = mask_name[0]
		last_name = mask_name[-1]
		middle_name = ' '.join(mask_name[1:-1])

		# nome abreviado
		self.nome_abreviado = ' '.join([last_name,first_name,middle_name[:1]]).strip()

		with requests.Session() as session:
			self.login(session)

			a = self.inteiro(session)
			if a:
				return

			b = self.abreviado(session)
			if b:
				return

		if a == False and b == False:
			self.sql_F(self.documento,self.nome)

	def get_token(self,response):

		token_soup = BeautifulSoup(response,'lxml')
		key = token_soup.find('meta',{'name':'csrf-token'})['content']
		return(key)


	def login(self,session):

		l = session.get('https://opencorporates.com/users/sign_in', timeout=30)

		token = self.get_token(l.text)

		login = {
		'utf8':'✓',
		'authenticity_token':token,
		'user[email]':'kevin.braga@jiveinvestments.com',
		'user[password]':'36968319kk',
		'submit':''
		}

		ll = session.post('https://opencorporates.com/users/sign_in',data=login, timeout=30)

		if ll.status_code == 200:
			return session
		else:
			logger.error("<ERROR: status_code:%s LOGIN DO OPENCORPORATES COM DEFEITO [%s]>"%(r.status_code,documento))
			return

		########## PESQUISA ##########

	def abreviado(self,session):

		mask_name = self.nome_abreviado.replace(' ','+')

		r = session.get('https://opencorporates.com/officers?q={0}&utf8=%E2%9C%93'.format(mask_name), timeout=30)

		if r.status_code == 200:

			soup = BeautifulSoup(r.text,'lxml')
			noresults = soup.find('p',{'id':'no_results'})

			if noresults:
				logger.debug("Nada localizado")
				return False

			else:
				results = soup.find('div',{'id':'results'})
				ul = results.find('ul',{'class':'officers unstyled'})
				li = ul.find_all('li')

				found = False

				for element in li:

					mask = element.find("a",{"class": lambda L: L and L.startswith('officer')})

					nome_encontrado = mask.text.replace(',','')

					percentage = fuzz.token_sort_ratio(self.nome, nome_encontrado)

					if percentage >= 90:

						print (nome_encontrado)

						if found:
							pass
						else:
							found = True

						opencorporates_url = ("https://opencorporates.com"+mask['href'])

						rr = session.get(opencorporates_url, timeout=30)
						if rr.status_code == 200:

							soupy = BeautifulSoup(rr.text,'lxml')
							dl = soupy.find('div',{'id':'attributes'}).find('dl',{'class':'attributes'})
							dd = dl.find_all('dd')

							company_status = company = company_url = address = position = occupation = nationality = start_date = None

							for element in dd:

								if element['class'][0] == "company":
									company = element.find('a').text
									company_url = element.find('a')['href']
									company_url = 'https://opencorporates.com'+company_url

									if element.find('span',{'class':'status label'}):
										company_status = 'inativo'
									else:
										company_status = 'ativo'

								if element['class'][0] == "address":
									address = element.text.replace('\n',' ')

								if element['class'][0] == "position":
									position = element.text

								if element['class'][0] == "occupation":
									occupation = element.text

								if element['class'][0] == "nationality":
									nationality = element.text

								if element['class'][0] == "start_date":
									start_date = element.text

							self.sql_T(self.documento,self.nome,opencorporates_url,company_status,company,company_url,address,position,occupation,nationality,start_date)
							#self.xml(self.documento,company,company_status,company_url,address,start_date)

				if found:
					return True
				return False

	def inteiro(self,session):

		mask_name = self.nome.replace(' ','+')

		r = session.get('https://opencorporates.com/officers?q={0}&utf8=%E2%9C%93'.format(mask_name), timeout=30)

		if r.status_code == 200:

			soup = BeautifulSoup(r.text,'lxml')
			noresults = soup.find('p',{'id':'no_results'})

			if noresults:
				logger.debug("Nada localizado")
				return False

			else:
				results = soup.find('div',{'id':'results'})
				ul = results.find('ul',{'class':'officers unstyled'})
				li = ul.find_all('li')

				found = False

				for element in li:

					mask = element.find("a",{"class": lambda L: L and L.startswith('officer')})

					nome_encontrado = mask.text.replace(',','')

					percentage = fuzz.token_sort_ratio(self.nome, nome_encontrado)

					print (nome_encontrado,percentage)

					if percentage >= 90:

						#print (nome_encontrado,percentage)

						if found:
							pass
						else:
							found = True

						opencorporates_url = ("https://opencorporates.com"+mask['href'])

						rr = session.get(opencorporates_url, timeout=30)
						if rr.status_code == 200:

							soupy = BeautifulSoup(rr.text,'lxml')
							dl = soupy.find('div',{'id':'attributes'}).find('dl',{'class':'attributes'})
							dd = dl.find_all('dd')

							company_status = company = company_url = address = position = occupation = nationality = start_date = None

							for element in dd:

								if element['class'][0] == "company":
									company = element.find('a').text
									company_url = element.find('a')['href']
									company_url = 'https://opencorporates.com'+company_url

									if element.find('span',{'class':'status label'}):
										company_status = 'inativo'
									else:
										company_status = 'ativo'

								if element['class'][0] == "address":
									address = element.text.replace('\n',' ')

								if element['class'][0] == "position":
									position = element.text

								if element['class'][0] == "occupation":
									occupation = element.text

								if element['class'][0] == "nationality":
									nationality = element.text

								if element['class'][0] == "start_date":
									start_date = element.text

							self.sql_T(self.documento,self.nome,opencorporates_url,company_status,company,company_url,address,position,occupation,nationality,start_date)
							#self.xml(self.documento,company,company_status,company_url,address,start_date)

				if found:
					return True
				return False


	def sql_T(self,documento,nome,opencorporates_url,company_status,company,company_url,address,position,occupation,nationality,start_date):

		import pyodbc
		
		try:
			banco = pyodbc.connect('.')
			cursor = banco.cursor()
			cursor.execute("""INSERT INTO DB_JIVE_2017.dbo.OPENCORPORATES(DOCUMENTO_PESSOA, NOME_PESSOA, OPENCORPORATES_URL, COMPANY_STATUS, COMPANY, COMPANY_URL, ADDRESS, POSITION, OCCUPATION, NATIONALITY, STARTDATE, DATA_INCLUSAO, CRITICA) 
			VALUES((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),getdate(),'Sucesso')""", documento, nome, opencorporates_url, company_status, company, company_url, address, position, occupation, nationality, start_date)
			banco.commit()
			cursor.close()			
			logger.debug("SQL_T bem sucedido")

		except pyodbc.Error as error:
			
			from modules import _email
			logger.error("<ERROR: %s [%s]>"%(error, documento))
			_email.send("OPENCORPORATES",error,documento)

		finally:
			banco.close()


	def sql_F(self,documento,nome):

		import pyodbc

		try:
			banco = pyodbc.connect('.')
			cursor = banco.cursor()
			cursor.execute("INSERT INTO DB_JIVE_2017.dbo.OPENCORPORATES(DOCUMENTO_PESSOA,NOME_PESSOA,DATA_INCLUSAO,CRITICA) VALUES((?),(?),getdate(),'Nada localizado')",documento,nome)
			banco.commit()
			cursor.close()			
			logger.debug("SQL_F bem sucedido")

		except pyodbc.Error as error:
			
			from modules import _email
			logger.error("<ERROR: %s [%s]>"%(error, documento))
			_email.send("OPENCORPORATES",error,documento)

		finally:
			banco.close()


	def xml(documento,company,company_status,company_url,address,start_date):

		from requests import Session
		from requests.auth import HTTPBasicAuth
		import zeep
		from zeep.transports import Transport

		session = Session()
		session.auth = HTTPBasicAuth(.')
		transport_with_basic_auth = Transport(session=session)

		client = zeep.Client(
			wsdl='https://jiveasset.service-now.com/x_jam_bd_opencorporates.do?WSDL',
			transport=transport_with_basic_auth
		)

		response = client.service.insert(
			u_cpf_cnpj = documento,
			u_endereco = adress,
			u_nome = company,
			u_status = company_status,
			u_url = company_url,
			u_data_de_inicio = start_date
		)
		
		logger.debug("<Insert: Registro inserido no ServiceNow [%s]>"%documento)


if __name__ == "__main__":

	o = Opencorporates('0001147806165','NELSON AFIF CURY')
	o.run()
