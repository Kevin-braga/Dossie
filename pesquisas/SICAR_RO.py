import logging
logger = logging.getLogger("Listener.call.SICAR_RO")
########################################################################
def run(documento,tipo_documento):

	import requests, json 
	from dateutil.parser import parse

	uf = "RO"

	if tipo_documento == "Jurídica":
		data = {'filtros[cpfCnpjDominio]': documento, 'filtros[pagina]:': '1'}

	elif tipo_documento == "Física":
		data = {'filtros[cpfCnpjDominio]': documento[3:], 'filtros[pagina]:': '1'}
		

	with requests.Session() as s:
		r = s.post("http://www.sedam.ro.gov.br/car/imovel/consultaByFiltros", data=data,timeout=30)
		

	if r.status_code == 200:

		data = (json.loads(r.text))
		msg = (data['mensagem'])

		if "Não existem dados a serem exibidos" in msg:
			logger.debug('Dados não encontrados')
			sql_F(documento, tipo_documento, uf)

		elif "Consulta realizada com sucesso" in msg:
			logger.debug('Dados encontrados')
			try:
				for i in data['dados']['lista']:

					area_imovel = (i['area'])
					cidade_imovel = (i['nomeMunicipio'])
					cod_car = (i['codigoImovel'])
					data_cadastro_car = (i['dataCadastro'])
					data_cadastro_car = parse(data_cadastro_car)
					mf_car = (i['modulosFiscais'])
					status_car = (i['status'])

					if status_car:

						if status_car == "AT":
							status_car	= "Ativo"

						if status_car == "CA":
							status_car = "Cancelado"

						if status_car == "PE":
							status_car = "Pendente"

					sql_T(documento, tipo_documento, uf, area_imovel, cidade_imovel, cod_car, data_cadastro_car, mf_car, status_car)
					xml (documento, tipo_documento, uf, area_imovel, cidade_imovel, cod_car, data_cadastro_car, mf_car, status_car)
			
			except (KeyError):
				return

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

	if tipo_documento == "Física":
		tipo_documento = "FISICA"
	elif tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"

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
		_email.send("SICAR_RO",error,documento)

	finally:
		banco.close()

def sql_F(documento, tipo_documento, uf):
	import pyodbc

	if tipo_documento == "Física":
		tipo_documento = "FISICA"
	elif tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"

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
		_email.send("SICAR_RO",error,documento)
	
	finally:
		banco.close()


	