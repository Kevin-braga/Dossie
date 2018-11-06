import logging
logger = logging.getLogger("Listener.call.TJRS")
########################################################################
def run(documento,tipo_documento):
	import requests
	from bs4 import BeautifulSoup
	from datetime import datetime

	uf = 'RS'

	if tipo_documento == "Jurídica":
		logger.debug('Documento validado como Júridico')
		url = "http://www.tjrs.jus.br/site_php/precatorios/lista_credores.php?aba_opcao_consulta=credores&busca_credores=&busca_cpf_cnpj={0}&rpv_precatorios=preca".format(documento)

	elif tipo_documento == "Física":
		logger.debug('Documento validado como Físico')
		url = "http://www.tjrs.jus.br/site_php/precatorios/lista_credores.php?aba_opcao_consulta=credores&busca_credores=&busca_cpf_cnpj={0}&rpv_precatorios=preca".format(documento[3:])
		

	with requests.Session() as s:
		r = s.get(url, timeout=30)
	
	if r.status_code == 200:

		logger.debug('GET_URL bem sucedido')
		soup = BeautifulSoup(r.text, 'lxml')
		links = soup.find_all("a")

		if links:
			logger.debug("Dados encontrados")

			for link in links:

				cod_precatorio = data_protocolo_precatorio = num_processo = cod_precatorio = origem_processo = devedor_precatorio = natureza_precatorio = situacao_precatorio = " "

				td = []
				rr = requests.get("http://www.tjrs.jus.br/site_php/precatorios/"+link['href'])
				soup = BeautifulSoup(rr.text, 'lxml')

				div = soup.find("div",{"id":"conteudo"})
				table = div.find_all("table")
				tr = table[0].find_all("tr")

				for element in tr:
					td.append(element.find_all("td"))
					
				for i in td:

					a = (i[0].text.strip())
					b = (i[1].text.strip())

					if "Número do Expediente" in a:
						cod_precatorio = b

					if "Data de Apresentação" in a:			
						data_protocolo_precatorio = datetime.strptime(b, '%d/%m/%Y')

					if "Processo Administrativo" in a:
						num_processo = b

					if a.startswith("Origem"):
						origem_processo = b

					if "Pagador" in a:
						devedor_precatorio = b

					if "Objeto" in a:
						natureza_precatorio = b

					if "Situação Atual" in a:
						situacao_precatorio = b

				xml (documento,tipo_documento,cod_precatorio,num_processo,devedor_precatorio,natureza_precatorio,situacao_precatorio,origem_processo,data_protocolo_precatorio,uf)	
				sql_T(documento,tipo_documento,cod_precatorio,num_processo,devedor_precatorio,natureza_precatorio,situacao_precatorio,origem_processo,data_protocolo_precatorio,uf)		

		else:
			logger.debug("Nada localizado")
			sql_F (documento,tipo_documento)
			return

	else:
		logger.error("<ERROR: status_code:%s [%s]>"%(r.status_code,documento))
		return

def xml(documento,tipo_documento,cod_precatorio,num_processo,devedor_precatorio,natureza_precatorio,situacao_precatorio,origem_processo,data_protocolo_precatorio,uf):

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
		u_natureza_precat_rio = natureza_precatorio,
		situa_o_precat_rio = situacao_precatorio,
		u_origem_processo_precatorio = origem_processo,
		u_data_protocolo_precat_rio = data_protocolo_precatorio,
		u_uf_precatorio = uf,
		u_cr_tica_precatorio = "Sucesso",
		u_fonte_pesquisa_precatorio = "TJRS"
		)

	logger.debug("<Insert: Registro inserido no ServiceNow [%s]>"%documento)


def sql_T(documento,tipo_documento,cod_precatorio,num_processo,devedor_precatorio,natureza_precatorio,situacao_precatorio,origem_processo,data_protocolo_precatorio,uf):
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
		DATA_PROTOCOLO_PRECATORIO,
		UF,
		DATA_INCLUSAO,
		CRITICA,
		FL_DEVEDOR_JIVE,
		FL_FASE_PRECIFICACAO,
		FONTE_PESQUISA
		) 
		VALUES ((?),(?),NULL,(?),(?),(?),(?),(?),(?),(?),(?),getdate(),'Dados do Precatorio capturados com Sucesso','N','N','TJRS')""",
		documento,tipo_documento,cod_precatorio,num_processo,devedor_precatorio,natureza_precatorio,situacao_precatorio,origem_processo,data_protocolo_precatorio,uf)
		banco.commit()
		cursor.close()          
		logger.debug("SQL_T bem sucedido")

	except pyodbc.Error as error:

		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("TJRS",error,documento)

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
		VALUES ((?),(?),NULL,getdate(),'Não existem precatórios para esta pessoa','N','N','TJRS')""",documento,tipo_documento)
		banco.commit()
		cursor.close()          
		logger.debug("SQL_F bem sucedido")

	except pyodbc.Error as error:

		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("TJRS",error,documento)

	finally:
		banco.close()
