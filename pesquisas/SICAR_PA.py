import logging
logger = logging.getLogger("Listener.call.SICAR_PA")
########################################################################
def run(documento,tipo_documento):

	import requests, json
	from dateutil.parser import parse

	uf = "PA"

	if tipo_documento == "Jurídica":
		url = "http://car.semas.pa.gov.br/site/imovel/consulta/geral/lista/filtros?cpfCnpjProprietario={0}&pagina=1".format(documento)

	elif tipo_documento == "Física":
		url = "http://car.semas.pa.gov.br/site/imovel/consulta/geral/lista/filtros?cpfCnpjProprietario={0}&pagina=1".format(documento[3:])


	with requests.Session() as s:
		r = s.get(url,timeout=30)
		

	if r.status_code == 200:

		data = json.loads(r.text)

		if data:

			logger.debug('Dados encontrados')
			
			for i in data:

				area_imovel = i.get('area')
				cidade_imovel = i.get('nomeMunicipio')
				cod_car = i.get('codigo')
				data_cadastro_car = i.get('dataCadastro')
				data_cadastro_car = parse(data_cadastro_car)
				mf_car = i.get('moduloFiscal')
				status_car = i.get('status')

				if status_car:

					if status_car == 'AT':
						status_car	= 'Ativo'
					elif status_car == 'CA':
						status_car = 'Cancelado'
					elif status_car == 'PE':
						status_car = 'Pendente'

				sql_T(documento, tipo_documento, uf, area_imovel, cidade_imovel, cod_car, data_cadastro_car, mf_car, status_car)
				xml (documento, tipo_documento, uf, area_imovel, cidade_imovel, cod_car, data_cadastro_car, mf_car, status_car)

		else:
			logger.debug('Dados não encontrados')
			sql_F(documento, tipo_documento, uf)
	
	else:
		logger.error("<ERROR: status_code:%s [%s]>"%(r.status_code,documento))
		return


def xml(documento,tipo_documento,uf, area_imovel, cidade_imovel, cod_car, data_cadastro_car, mf_car, status_car):
	from requests import Session
	from requests.auth import HTTPBasicAuth
	import zeep
	from zeep.transports import Transport

	session = Session()
	session.auth = HTTPBasicAuth(.')
	transport_with_basic_auth = Transport(session=session)

	client = zeep.Client(
		wsdl='https://jiveasset.service-now.com/x_jam_bd_sicar.do?wsdl',
		transport=transport_with_basic_auth
	)

	response = client.service.insert(

		u_cpf_cnpj = documento,
		u_tipo_pessoa_sicar = tipo_documento,
		uf = uf,
		u_area_imovel_sicar = area_imovel,
		cidade = cidade_imovel,
		u_cod_car_sicar = cod_car,
		u_mf_car = mf_car,
		u_status_sicar = status_car,
		#u_data_cadastro = data_cadastro_car
	)
	logger.debug("<Insert: Registro inserido no ServiceNow [%s]>"%documento)

def sql_T(documento, tipo_documento, uf, area_imovel, cidade_imovel, cod_car, data_cadastro_car, mf_car, status_car):
	import pyodbc

	if tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"
	elif tipo_documento == "Física":
		tipo_documento = "FISICA"

	try:
		banco = pyodbc.connect('.')
		cursor = banco.cursor()
		cursor.execute("insert into DB_JIVE_2017.dbo.IMOVEIS_CAR(DOCUMENTO_PROPRIETARIO, TIPO_PESSOA, UF, AREA_IMOVEL, CIDADE_IMOVEL, COD_CAR, DATA_CADASTRO_CAR, STATUS_CAR, MF_CAR , DATA_INCLUSAO, CRITICA) values((?),(?),(?),(?),(?),(?),(?),(?),(?),getdate(), 'Dados capturados com sucesso')", documento, tipo_documento, uf, area_imovel, cidade_imovel, cod_car, data_cadastro_car, status_car, mf_car)
		banco.commit()
		cursor.close()			
		logger.debug("SQL_T bem sucedido")

	except pyodbc.Error as error:
		
		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("SICAR_PA",error,documento)

	finally:
		banco.close()


def sql_F(documento, tipo_documento, uf):
	import pyodbc

	if tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"
	elif tipo_documento == "Física":
		tipo_documento = "FISICA"

	try:	
		banco = pyodbc.connect('.')
		cursor = banco.cursor()
		cursor.execute("insert into DB_JIVE_2017.dbo.IMOVEIS_CAR(DOCUMENTO_PROPRIETARIO, TIPO_PESSOA, UF, AREA_IMOVEL, CIDADE_IMOVEL, COD_CAR, DATA_CADASTRO_CAR, STATUS_CAR, MF_CAR , DATA_INCLUSAO, CRITICA) values((?),(?),(?),NULL,NULL,NULL,NULL,NULL,NULL,getdate(), 'Não existe cadastro para esta pessoa')", documento, tipo_documento, uf)
		banco.commit()
		cursor.close()			
		logger.debug("SQL_F bem sucedido")

	except pyodbc.Error as error:
		
		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("SICAR_PA",error,documento)

	finally:
		banco.close()

