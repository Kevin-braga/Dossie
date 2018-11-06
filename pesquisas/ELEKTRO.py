import logging
logger = logging.getLogger("Listener.call.ELEKTRO")

########################################################################

def run(documento,tipo_documento):
	import requests
	from bs4 import BeautifulSoup

	if tipo_documento == "Jurídica":
		url = "https://pagamentodigital.elektro.com.br/UC?codigo={0}".format(documento)

	elif tipo_documento == "Física":
		url = "https://pagamentodigital.elektro.com.br/UC?codigo={0}".format(documento[3:])

	r = requests.get(url, timeout=60)
	if r.status_code == 200:

		soup = BeautifulSoup(r.text,'lxml')

		table = soup.find('table')

		if table:

			tr = table.find_all('tr')

			for td in tr[1:]:

				element = td.find_all('td')
				logger.debug('Dados encontrados')

				if element[4]:

					codigo = bairro = municipio = mask = numero = logradouro = ' '

					# código imobiliario
					codigo = element[1].text.strip()
					# bairro
					bairro = element[3].text.strip()
					# municipio	
					municipio = element[4].text.strip()
					# logradouro / endereço
					mask = element[2].text.strip()

					if "," in mask:

						numero = mask.split(",")[1].strip()
						logradouro = mask.split(",")[0].strip()

						if "-" in numero:
							numero = numero.rsplit('-')[0].strip()

					else:
						logradouro = mask

					print(documento, "SALVO")
					xml(documento,tipo_documento,codigo,logradouro,numero,bairro,municipio)
					sql_T(documento,tipo_documento,codigo,logradouro,numero,bairro,municipio)
					return

				else:

					div_dados = soup.find('div',{'class':'col-md-12'})

					p = div_dados.find('p')

					if p:

						codigo = bairro = municipio = mask = numero = logradouro = ' '

						strong = p.find('strong')

						parent = strong.parent.text

						dados = parent.split('\n')
						
						for d in dados:

							if "Código:" in d:
								codigo = d.split('Código:')[1].strip()
							
							if "Endereço:" in d:
								mask = d.split('Endereço:')[1].strip()

								if "," in mask:

									numero = mask.split(",")[1].strip()
									logradouro = mask.split(",")[0].strip()

									if "-" in numero:
										numero = numero.rsplit('-')[0].strip()
				
						xml(documento,tipo_documento,codigo,logradouro,numero,bairro,municipio)
						sql_T(documento,tipo_documento,codigo,logradouro,numero,bairro,municipio)
						return
		
		logger.debug("Nada localizado")
		sql_F (documento,tipo_documento)

	else:
		logger.error("<ERROR: status_code:%s [%s]>"%(r.status_code,documento))
		return


def xml(documento,tipo_documento,codigo,logradouro,numero,bairro,municipio):
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
		u_logradouro_energeticas = logradouro,
		u_n_mero_energeticas = numero,
		u_bairro_energeticas = bairro,
		u_cidade_energeticas = municipio,
		u_descri_o_centrais = "Código imobiliário : %s" %codigo
	)

	logger.debug("<Insert: Registro inserido no ServiceNow [%s]>"%documento)


def sql_T(documento,tipo_documento,mask,logradouro,numero,bairro,municipio):
	import pyodbc

	if tipo_documento == "Física":
		tipo_documento = "FISICA"
	elif tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"

	try:
		banco = pyodbc.connect('.')
		cursor = banco.cursor()
		cursor.execute("""insert into db_jive_2017.dbo.CENTRAIS_ENERGETICAS(DOCUMENTO_PESSOA, TIPO_PESSOA, ENDERECO_COMPLETO, LOGRADOURO, NUMERO, BAIRRO, CIDADE, DATA_INCLUSAO, CRITICA, FONTE_PESQUISA) 
		values((?),(?),(?),(?),(?),(?),(?),getdate(),'Sucesso','ELEKTRO')""", documento, tipo_documento, mask, logradouro, numero, bairro, municipio)
		banco.commit()
		cursor.close()			
		logger.debug("SQL_T bem sucedido")

	except pyodbc.Error as error:
		
		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("ELEKTRO",error,documento)

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
		cursor.execute("insert into db_jive_2017.dbo.CENTRAIS_ENERGETICAS(Documento_PESSOA, tipo_Pessoa, Data_Inclusao, Critica, FONTE_PESQUISA) values((?),(?),getdate(), 'Não existe registro para este documento','ELEKTRO')",
		documento, tipo_documento)
		banco.commit()
		cursor.close()			
		logger.debug("SQL_F bem sucedido")

	except pyodbc.Error as error:
		
		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("ELEKTRO",error,documento)

	finally:
		banco.close()


if __name__ == "__main__":

	run ('58783606000120', 'Jurídica')
