import logging
logger = logging.getLogger("Listener.call.SIGEF")
########################################################################
def run(documento,tipo_documento):
	import requests
	from bs4 import BeautifulSoup

	if tipo_documento == "Física":
		url = "https://sigef.incra.gov.br/consultar/parcelas/?termo=&pesquisa_avancada=True&cpf_cnpj={0}".format(documento[3:])

	elif tipo_documento == "Jurídica":
		url = "https://sigef.incra.gov.br/consultar/parcelas/?termo=&pesquisa_avancada=True&cpf_cnpj={0}".format(documento)


	with requests.Session() as s:
		r = s.get(url, timeout=30,verify=False)

	if r.status_code == 200:

		logger.debug("GET_URL bem sucedido")

		soup = BeautifulSoup(r.text, 'lxml')

		table = soup.find("tbody",{"id": "tbl-parcelas tbody"})

		if table:
			logger.debug("Dados localizados")
			pass
		else:
			sql_F(documento,tipo_documento)
			logger.debug("Nada encontrado")
			return
	
		tr = table.find_all("tr")

		links = []

		for i in tr:

			td = i.find_all("td", recursive=False)
			links.append(td[0].a['href'])

		for link in links:

			detentor = area = data_de_entrada = situacao_parcela = denominacao = responsavel_tecnico = art = cartorio = municipio = uf = cns = matricula = situacao_registro = ""

			parseurl = "https://sigef.incra.gov.br"+link			
			rr = requests.get(parseurl, timeout=30,verify=False)
			soup = BeautifulSoup(rr.text, 'lxml')
			div = soup.find_all("div",{"class":"panel dv-expandable"})

			for element in div:

				if "Identificação do detentor" in element.div.text:

					headers = element.find_all("th")
					values = element.find_all("td")

					for a,b in zip(headers,values):

						a = a.text.strip()
						b = b.text.strip()

						if "Nome" in a:
							detentor = b

				if "Informações da parcela" in element.div.text:
					headers = element.find_all("th")
					values = element.find_all("td")

					for a,b in zip(headers,values):

						a = a.text.strip()
						b = b.text.strip()

						if "Área" in a:
							area = b
					
						if "Data de Entrada" in a:
							data_de_entrada = b

						if "Situação" in a:
							situacao_parcela = b

						if "Denominação" in a:
							denominacao = b

						if "Responsável Técnico" in a:
							responsavel_tecnico = b

						if "ART" in a:
							art = b

				if "Informações de Registro" in element.div.text:
					headers = element.find_all("th")
					values = element.find_all("td")
					
					for a,b in zip(headers,values):
						
						a = a.text.strip()
						b = b.text.strip()

						if "Cartório" in a:
							cartorio = b

						if "Município - UF" in a:

							if "-" in b:
								mask_b = b.split('-')

								municipio = mask_b[0].strip()
								uf = mask_b[1].strip()

							else:
								municipio = b.strip()

						if "Código Nacional de Serventia" in a:
							cns = b

						if "Matrícula" in a:
							matricula = b

						if "Situação do Registro" in a:
							situacao_registro = b

			sql_T(documento,tipo_documento,detentor,area,data_de_entrada,situacao_parcela,denominacao,responsavel_tecnico,art,cartorio,municipio,uf,cns,matricula,situacao_registro)
			xml(documento,tipo_documento,detentor,area,data_de_entrada,situacao_parcela,denominacao,responsavel_tecnico,art,cartorio,municipio,uf,cns,matricula,situacao_registro)

	else:
		logger.error("<ERROR: status_code:%s [%s]>"%(r.status_code,documento))
		return

def sql_T(documento,tipo_documento,detentor,area,data_de_entrada,situacao_parcela,denominacao,responsavel_tecnico,art,cartorio,municipio,uf,cns,matricula,situacao_registro):
	import pyodbc

	if tipo_documento == "Física":
		tipo_documento = "FISICA"
	elif tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"

	try:
		banco = pyodbc.connect('.')
		cursor = banco.cursor()
		cursor.execute("""INSERT INTO DB_JIVE_2017.dbo.SIGEF(DOCUMENTO_PROPRIETARIO,TIPO_PESSOA,NOME_PESSOA,DETENTOR,AREA,DATA_ENTRADA,SITUACAO_PARCELA,DENOMINACAO,RESPONSAVEL_TECNICO,ART,CARTORIO,MUNICIPIO,UF,CNS,MATRICULA,SITUACAO_REGISTRO,DATA_INCLUSAO,CRITICA) 
		VALUES((?),(?),NULL,(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),getdate(), 'Sucesso')""",documento,tipo_documento,detentor,area,data_de_entrada,situacao_parcela,denominacao,responsavel_tecnico,art,cartorio,municipio,uf,cns,matricula,situacao_registro)

		banco.commit()
		cursor.close()			
		logger.debug("SQL_T bem sucedido")

	except pyodbc.Error as error:

		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("SIGEF",error,documento)

	finally:
		banco.close()

def sql_F(documento,tipo_documento):
	import pyodbc

	if tipo_documento == "Física":
		tipo_documento = "FISICA"
	elif tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"

	try:
		banco = pyodbc.connect('.')
		cursor = banco.cursor()
		cursor.execute("""INSERT INTO DB_JIVE_2017.dbo.SIGEF(DOCUMENTO_PROPRIETARIO,TIPO_PESSOA,NOME_PESSOA,DETENTOR,AREA,DATA_ENTRADA,SITUACAO_PARCELA,DENOMINACAO,RESPONSAVEL_TECNICO,ART,CARTORIO,MUNICIPIO,UF,CNS,MATRICULA,SITUACAO_REGISTRO,DATA_INCLUSAO,CRITICA) 
		VALUES((?),(?),NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,getdate(), 'Registro não encontrado')""",documento,tipo_documento)
		banco.commit()
		cursor.close()			
		logger.debug("SQL_F bem sucedido")

	except pyodbc.Error as error:

		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("SIGEF",error,documento)

	finally:		
		banco.close()


def xml(documento,tipo_documento,detentor,area,data_de_entrada,situacao_parcela,denominacao,responsavel_tecnico,art,cartorio,municipio,uf,cns,matricula,situacao_registro):

	from requests import Session
	from requests.auth import HTTPBasicAuth
	import zeep
	from zeep.transports import Transport

	session = Session()
	session.auth = HTTPBasicAuth(.')
	transport_with_basic_auth = Transport(session=session)

	client = zeep.Client(
		wsdl='https://jiveasset.service-now.com/x_jam_bd_sigef_list.do?WSDL',
		transport=transport_with_basic_auth
	)

	response = client.service.insert(
		u_cpf_cnpj = documento,
		tipo_pessoa = tipo_documento,
		detentor = detentor,
		area = area,
		data_entrada = data_de_entrada,
		situacao_parcela = situacao_parcela,
		denominacao = denominacao,
		responsavel_tecnico = responsavel_tecnico,
		art = art,
		cartorio = cartorio,
		municipio = municipio,
		uf = uf,
		cns = cns,
		matricula = matricula,
		situacao_registro = situacao_registro
		)

	logger.debug("<Insert: Registro inserido no ServiceNow [%s]>"%documento)
