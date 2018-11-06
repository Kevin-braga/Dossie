import requests, json, os, logging
from bs4 import BeautifulSoup
from fuzzywuzzy import fuzz

logger = logging.getLogger("Listener.call.SUNBIZ")

class Sunbiz():

	def __init__(self,d,n):

		self.folder = "W:\\1. Projetos\\3. Mineração de Dados\\Sunbiz\\{0}"
		self.documento = d
		self.nome = n

	def run(self):

		#	separando nome por espaço
		mask_name = self.nome.split(' ')

		#	coletando nome,sobrenome,meionome
		first_name = mask_name[0]
		last_name = mask_name[-1]
		middle_name = ' '.join(mask_name[1:-1])

		# nome abreviado
		abbreviated_name = ' '.join([last_name,first_name,middle_name[:1]]).strip()

		# nome reorganizado
		reorganized_name = ' '.join([last_name,first_name,middle_name]).strip()	

		a = self.reorganized(reorganized_name)
		if a:
			return

		b = self.abbreviated(abbreviated_name)
		if b:
			return

		# sql_F
		if a == False and b == False:
			print('nada')
			self.sql_F(self.documento,self.nome)


	def shortner(self,long_url):

		post_url = 'https://www.googleapis.com/urlshortener/v1/url?key=AIzaSyDEA2g16wFQpihPZhRSlJCbngMHqGtHsVs'
		payload = {'longUrl': long_url}
		headers = {'content-type': 'application/json'}
		k = requests.post(post_url, data=json.dumps(payload), headers=headers, timeout=30)
		data = json.loads(k.text)
		short_url = (data['id'])

		return short_url


	def reorganized(self,reorganized_name):

		r = requests.get("http://search.sunbiz.org/Inquiry/CorporationSearch/SearchResults/OfficerRegisteredAgentName/{0}/Page1".format(reorganized_name) ,timeout=60)

		if r.status_code == 200:
			soup = BeautifulSoup(r.text,'lxml')
			results = soup.find('div',{'id':'search-results'})
			tr = results.find_all('tr')

			found = False

			for i in tr:
				td = i.find_all('td')
				if td:
					officer_name = td[0].text
					officer_name = officer_name.replace(".","").replace(",","").strip()

					percentage = fuzz.token_sort_ratio(self.nome, officer_name)

					print (self.nome, ':', officer_name)
					print(percentage)

					if percentage > 80:

						print(fuzz.partial_ratio(self.nome, officer_name))

			# 		if officer_name == reorganized_name:

			# 			if found:
			# 				pass
			# 			else:
			# 				found = True

			# 			url = 'http://search.sunbiz.org'+td[0].a['href']
			# 			sunbiz_url = self.shortner(url)
			# 			self.parse(sunbiz_url)


			# if not found:
			# 	return False

			# return True
	
	def abbreviated(self,abbreviated_name):
		
		r = requests.get("http://search.sunbiz.org/Inquiry/CorporationSearch/SearchResults/OfficerRegisteredAgentName/{0}/Page1".format(abbreviated_name) ,timeout=60)

		if r.status_code == 200:
			
			soup = BeautifulSoup(r.text,'lxml')
			results = soup.find('div',{'id':'search-results'})
			tr = results.find_all('tr')

			found = False

			for i in tr:
				td = i.find_all('td')
				if td:
					officer_name = td[0].text
					officer_name = officer_name.replace(".","").replace(",","").strip()

					percentage = fuzz.token_sort_ratio(self.nome, officer_name)

					print(percentage)

			# 		if officer_name == abbreviated_name:

			# 			if found:
			# 				pass
			# 			else:
			# 				found = True

			# 			url = 'http://search.sunbiz.org'+td[0].a['href']
			# 			sunbiz_url = self.shortner(url)
			# 			self.parse(sunbiz_url)


			# if not found:
			# 	return False

			# return True

	def parse(self,sunbiz_url):

		company = company_state = company_status = document_number = fein_number = date_filed = last_event = event_date_filed = main_address = registered_agent_name = registered_agent_address = ''

		rr = requests.get(sunbiz_url,timeout=60)
		soupy = BeautifulSoup(rr.text,'lxml')

		div_detailSection = []
		section = soupy.find_all('div',{'class':'detailSection'})
		for div in section:

			if div['class'] == ['detailSection', 'corporationName']:
				div_corporationName = div

			elif div['class'] == ['detailSection', 'filingInformation']:
				div_filingInformation = div

			elif div['class'] == ['detailSection']:
				div_detailSection.append(div)


		######### Corporation Name ###########
		p = div_corporationName.find_all('p')			
		company = p[1].text


		################ filingInformation ###############
		div = div_filingInformation.find('div')

		labels = div.find_all('label')
		spans = div.find_all('span')

		for label,span in zip(labels,spans):

			label = label.text

			if label == 'Document Number':
				document_number = span.text
			if label == 'FEI/EIN Number':
				fein_number = span.text
			if label == 'Date Filed':
				date_filed = span.text
			if label == 'State':
				company_state = span.text
			if label == 'Status':
				company_status = span.text
			if label == 'Last Event':
				last_event = span.text
			if label == 'Event Date Filed':
				event_date_filed = span.text


		################## Adress ###################
		for i in div_detailSection:
		
			reserve = []
			element = i.span.text

			#	Principal Address

			if element == 'Principal Address':

				main_address = i.find('div')
				if main_address:
					mask_address = main_address.text.split('\r\n')
					for i in mask_address:
						if i:
							reserve.append(i.strip())

					main_address = [x for x in reserve]
					main_address = ' '.join(main_address)


			#	Registered Agent Name & Address

			if element == 'Registered Agent Name & Address':

				registered_agent_name = i.span.findNext('span')
				registered_agent_address = i.find('div')

				if registered_agent_name:
					registered_agent_name = registered_agent_name.text.strip()

				if registered_agent_address:
					mask_agent_address = registered_agent_address.text.split('\r\n')
					for i in mask_agent_address:
						if i:
							reserve.append(i.strip())

					registered_agent_address = [x for x in reserve]
					registered_agent_address = ' '.join(registered_agent_address)


			#	Document Images

			if element == 'Document Images':
				
				mask_tr = i.find_all('tr')
				for tr in mask_tr:
					a = tr.find('a')
					if a:
						file_name = a.text.replace('/','-')
						pdf_url = (a['href'])
						pdf_r = requests.get("http://search.sunbiz.org" + pdf_url, stream=True)

						if not os.path.exists(self.folder.format(self.nome)):
							os.makedirs(self.folder.format(self.nome))

						with open (self.folder.format(self.nome) + "\\%s.pdf"%file_name , 'wb') as f:
							f.write(pdf_r.content)

		# sql_T
		self.sql_T(self.documento,self.nome,sunbiz_url,company,company_state,company_status,document_number,fein_number,date_filed,last_event,event_date_filed,main_address,registered_agent_name,registered_agent_address)
		
		# xml
		self.xml(self.documento,company,company_status,company_url,address,start_date)

		return

	def sql_T(self,documento,nome,sunbiz_url,company,company_state,company_status,document_number,fein_number,date_filed,last_event,event_date_filed,main_address,registered_agent_name,registered_agent_address):

		import pyodbc
		
		try:
			banco = pyodbc.connect('.')
			cursor = banco.cursor()
			cursor.execute("""INSERT INTO DB_JIVE_2017.dbo.SUNBIZ(
			DOCUMENTO_PESSOA, NOME_PESSOA, SUNBIZ_URL,COMPANY,COMPANY_STATE,COMPANY_STATUS,DOCUMENT_NUMBER,FEIN_NUMBER,DATE_FILED,LAST_EVENT,EVENT_DATE_FILED,MAIN_ADDRESS,REGISTERED_AGENT_NAME,REGISTERED_AGENT_ADRESS,DATA_INCLUSAO,CRITICA)
			VALUES((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),getdate(),'Sucesso')""", documento,nome,sunbiz_url,company,company_state,company_status,document_number,fein_number,date_filed,last_event,event_date_filed,main_address,registered_agent_name,registered_agent_address)
			banco.commit()
			cursor.close()			
			logger.debug("SQL_T bem sucedido")

		except pyodbc.Error as error:
			
			from modules import _email
			logger.error("<ERROR: %s [%s]>"%(error, documento))
			_email.send("SUNBIZ",error,documento)

		finally:
			banco.close()

	def sql_F(self,documento,nome):

		import pyodbc
		
		try:
			banco = pyodbc.connect('.')
			cursor = banco.cursor()
			cursor.execute("INSERT INTO DB_JIVE_2017.dbo.SUNBIZ(DOCUMENTO_PESSOA,NOME_PESSOA,DATA_INCLUSAO,CRITICA) VALUES((?),(?),getdate(),'Nada localizado')", documento,nome)
			banco.commit()
			cursor.close()			
			logger.debug("SQL_F bem sucedido")

		except pyodbc.Error as error:
			
			from modules import _email
			logger.error("<ERROR: %s [%s]>"%(error, documento))
			_email.send("SUNBIZ",error,documento)

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
			wsdl='https://jiveasset.service-now.com/x_jam_bd_sunbiz.do?WSDL',
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

	sunbiz = Sunbiz('00044785395885','NELSON AFIF CURY')
	sunbiz.run()
