import logging
logger = logging.getLogger("Listener.call.TRF5")
########################################################################
def run(documento,tipo_documento):
	import requests
	from bs4 import BeautifulSoup
	from datetime import datetime

	if tipo_documento == "Jurídica":
		logger.debug('Documento validado como Júridico')

		data = {
		'navigation':'Netscape',
		'tipo':'xmlrpvprec',
		'filtro_CPFCNPJ':documento,
		'tipoproc':'T',
		'ordenacao':'N',
		'vinculados':'true',
		'Pesquisar':'Pesquisar'
		}

	elif tipo_documento == "Física":
		logger.debug('Documento validado como Físico')
		
		data = {
		'navigation':'Netscape',
		'tipo':'xmlrpvprec',
		'filtro_CPFCNPJ':documento[3:],
		'tipoproc':'T',
		'ordenacao':'N',
		'vinculados':'true',
		'Pesquisar':'Pesquisar'
		}


	url = "http://www.trf5.jus.br/cp/cp.do"

	with requests.Session() as s:
		r = s.post(url, data=data, timeout=30)
	
	if r.status_code == 200:
		logger.debug('GET_URL bem sucedido')

		soup = BeautifulSoup(r.text, 'lxml')
		table = soup.find("table",{"class": "consulta_resultados"})

		if table:
			logger.debug('Dados encontrados')

			td = table.find_all("td",{"id": lambda L: L and L.startswith('result')})

			for element in td:

				num_processo = " "
				cod_precatorio = " "
				data_movimento = " "
				desc_ultimo_movimento = " "
				uf = " "
				tipo_pagto = " "

				tag = (element.find_next_siblings())
				num_processo = tag[0].text
				cod_precatorio = tag[1].text
				data_movimento = datetime.strptime((tag[2].text),'%d/%m/%Y')
				desc_ultimo_movimento = tag[4].text

				mask = cod_precatorio.split('-')
				uf = mask[1].rsplit(" ")
				uf = uf[0]

				if "RPV" in cod_precatorio:
					tipo_pagto = "RPV"
				elif "PRC" in cod_precatorio:
					tipo_pagto = "PRC"

				xml (documento, tipo_documento, cod_precatorio, num_processo, tipo_pagto, uf, data_movimento, desc_ultimo_movimento)
				sql_T(documento, tipo_documento, cod_precatorio, num_processo, tipo_pagto, uf, data_movimento, desc_ultimo_movimento)

			return

		else:
			sql_F(documento, tipo_documento)
			return
	
	else:
		logger.error("<ERROR: status_code:%s [%s]>"%(r.status_code,documento))
		return
		
def xml(documento, tipo_documento, cod_precatorio, num_processo, tipo_pagto, uf, data_movimento, desc_ultimo_movimento):

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
		u_cod_precat_rio = cod_precatorio,
		u_tipo_pagamento_precatorio = tipo_pagto,
		u_uf_precatorio = uf,
		u_data_movimento_precatorio = data_movimento,
		u_n_mero_processo_precatorio = num_processo,
		u_descri_o_ultimo_movimento = desc_ultimo_movimento,
		u_cr_tica_precatorio = "Sucesso",
		u_fonte_pesquisa_precatorio = "TRF5"
	)
	logger.debug("<Insert: Registro inserido no ServiceNow [%s]>"%documento)


def sql_T(documento, tipo_documento, cod_precatorio, num_processo, tipo_pagto, uf, data_movimento, desc_ultimo_movimento):
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
		COD_PRECATORIO,
		NUM_PROCESSO,
		TIPO_PAGTO,
		UF,
		DATA_MOVIMENTO,
		DESC_ULTIMO_MOVIMENTO,
		DATA_INCLUSAO,
		CRITICA,
		FL_DEVEDOR_JIVE,
		FL_FASE_PRECIFICACAO,
		FONTE_PESQUISA
		) 
		VALUES ((?),(?),NULL,(?),(?),(?),(?),(?),(?),getdate(),'Dados do Precatorio capturados com Sucesso','N','N','TRF5')""",
		documento, tipo_documento, cod_precatorio, num_processo, tipo_pagto, uf, data_movimento, desc_ultimo_movimento)
		banco.commit()
		cursor.close()  
		logger.debug("SQL_T bem sucedido")        

	except pyodbc.Error as error:

		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("TRF5",error,documento)

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
		VALUES ((?),(?),NULL,getdate(),'Não existem precatórios para esta pessoa','N','N','TRF5')""",documento,tipo_documento)
		banco.commit()
		cursor.close()
		logger.debug("SQL_F bem sucedido")          

	except pyodbc.Error as error:

		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("TRF5",error,documento)

	finally:
		banco.close()

