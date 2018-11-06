import logging
logger = logging.getLogger("Listener.call.TRF2")
########################################################################
def run(documento, tipo_documento):
	import requests
	from bs4 import BeautifulSoup

	if tipo_documento == "Jurídica":
		url = "http://www2.trf2.jus.br/trf2requisitorioweb/ListaPesquisaPublico.aspx?cpf={0}&modulo=g&TipoBusca=CPFCNPJ".format(documento)

	elif tipo_documento == "Física":
		url = "http://www2.trf2.jus.br/trf2requisitorioweb/ListaPesquisaPublico.aspx?cpf={0}&modulo=g&TipoBusca=CPFCNPJ".format(documento[3:])
		

	with requests.Session() as s:
		r = s.post(url, timeout=30)
	
	if r.status_code == 200:

		logger.debug('GET_URL bem sucedido')
		soup = BeautifulSoup(r.text, 'lxml')
		links = soup.find_all("a",{"id": lambda L: L and L.startswith('dtgLista')})

		if links:

			logger.debug("Dados encontrados")

			for link in links:

				cod_precatorio = link.string
				href = link['href']

				pdf_path = r'C:\files\prd\codes\arquivos\{0}.pdf'.format(documento)

				rr = requests.get("http://www2.trf2.jus.br/trf2requisitorioweb/"+href, timeout=30)
				with open(pdf_path, 'wb') as f:
					f.write(rr.content)

				dados = read_file(pdf_path)
				
				num_processo = dados[0]
				devedor_precatorio = dados[1]
				natureza = dados[2]
				situacao_precatorio = dados[3]
				origem_processo = dados[4]
				tipo_causa = dados[5]
				data_liquidacao = dados[6]
				data_protocolo = dados[7]
				tipo_pagto = dados[8]

				xml (documento,tipo_documento,cod_precatorio,num_processo,devedor_precatorio,natureza,situacao_precatorio,origem_processo,tipo_causa,data_liquidacao,data_protocolo,tipo_pagto)	
				sql_T(documento,tipo_documento,cod_precatorio,num_processo,devedor_precatorio,natureza,situacao_precatorio,origem_processo,tipo_causa,data_liquidacao,data_protocolo,tipo_pagto)	

		else:
			logger.debug("Nada localizado")
			sql_F (documento,tipo_documento)

	else:
		logger.error("<ERROR: status_code:%s [%s]>"%(r.status_code,documento))
		return

def read_file(pdf_path):

	from tabula import read_pdf
	from datetime import datetime

	df = read_pdf(pdf_path,  pages='all' ,encoding = "ISO-8859-1")

	data = df.values.T.tolist()

	### LIMPANDO VARIAVEIS ###
	num_processo=devedor_precatorio=natureza=situacao_precatorio=origem_processo=tipo_pagto=data_liquidacao=data_protocolo=tipo_causa=" "

	for col1,col2 in zip (data[0],data[1]):


		if "Número do Processo" in col1:
			num_processo = col2.strip()
		
		if "Réu" in col1:
			devedor_precatorio = col2.strip()
		
		if "Natureza de Crédito" in col1:
			natureza = col2.strip()

		if "Situação do Requisitório:" in col1:
			situacao_precatorio = col2.strip()

		if "Oficiante" in col1:
			origem_processo = col2.strip()

		if "Espécie da Requisição" in col1:
			tipo_pagto = col2.strip()

		if "Data do último depósito" in col1:
			data_liquidacao = col2.strip()
			data_liquidacao = datetime.strptime(data_liquidacao, '%d/%m/%Y')

		if "Protocolado no Tribunal" in col1:
			data_protocolo = col2.strip()
			data_protocolo = datetime.strptime(data_protocolo, '%d/%m/%Y')

		if "Natureza da Obrigação" in col1:
			tipo_causa = col2.strip()

	
	dados = [num_processo,devedor_precatorio,natureza,situacao_precatorio,origem_processo,tipo_causa,data_liquidacao,data_protocolo,tipo_pagto]
	return (dados)

def xml(documento,tipo_documento,cod_precatorio,num_processo,devedor_precatorio,natureza,situacao_precatorio,origem_processo,tipo_causa,data_liquidacao,data_protocolo,tipo_pagto):

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
		u_n_mero_processo_precatorio = num_processo,
		u_devedor_precat_rio = devedor_precatorio,
		u_natureza_precat_rio = natureza,
		u_descri_o_protocolo_precatorio = situacao_precatorio,
		u_origem_processo_precatorio = origem_processo,
		u_tipo_causa_precatorio = tipo_causa,
		u_data_liquida_o_precat_rio = data_liquidacao,
		u_data_protocolo_precat_rio = data_protocolo,
		u_tipo_pagamento_precatorio = tipo_pagto,
		u_cr_tica_precatorio = "Sucesso",
		u_fonte_pesquisa_precatorio = "TRF2"
		)

	logger.debug("<Insert: Registro inserido no ServiceNow [%s]>"%documento)


def sql_T(documento,tipo_documento,cod_precatorio,num_processo,devedor_precatorio,natureza,situacao_precatorio,origem_processo,tipo_causa,data_liquidacao,data_protocolo,tipo_pagto):
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
		DEVEDOR_PRECATORIO,
		NATUREZA_PRECATORIO,
		SITUACAO_PRECATORIO,
		ORIGEM_PROCESSO,
		TIPO_CAUSA,
		DATA_LIQUIDACAO_PRECATORIO,
		DATA_PROTOCOLO_PRECATORIO,
		TIPO_PAGTO,
		DATA_INCLUSAO,
		CRITICA,
		FL_DEVEDOR_JIVE,
		FL_FASE_PRECIFICACAO,
		FONTE_PESQUISA
		) 
		VALUES ((?),(?),NULL,(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),getdate(),'Dados do Precatorio capturados com Sucesso','N','N','TRF2')""",
		documento,tipo_documento,cod_precatorio,num_processo,devedor_precatorio,natureza,situacao_precatorio,origem_processo,tipo_causa,data_liquidacao,data_protocolo,tipo_pagto)

		banco.commit()
		cursor.close()          
		logger.debug("SQL_T bem sucedido")

	except pyodbc.Error as error:

		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("TRF2",error,documento)

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
		VALUES ((?),(?),NULL,getdate(),'Não existem precatórios para esta pessoa','N','N','TRF2')""",documento,tipo_documento)
		banco.commit()
		cursor.close()          
		logger.debug("SQL_F bem sucedido")

	except pyodbc.Error as error:

		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("TRF2",error,documento)

	finally:
		banco.close()

