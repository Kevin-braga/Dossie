import logging
logger = logging.getLogger("Listener.call.SICAR_MT")

def run(documento,tipo_documento):

	import requests, json
	from dateutil.parser import parse

	uf = 'MT'

	if tipo_documento == "Jurídica":
		url = 'http://monitoramento.sema.mt.gov.br/simcarconsulta/api/api/Car/BuscarPorProprietario?cpfcnpj={0}'.format(documento)

	elif tipo_documento == "Física":
		url = 'http://monitoramento.sema.mt.gov.br/simcarconsulta/api/api/Car/BuscarPorProprietario?cpfcnpj={0}'.format(documento[3:])
	

	with requests.Session() as s:
		r = s.get(url, timeout=30)

	if r.status_code == 200:

		data = json.loads(r.text)

		if data:

			logger.debug('Dados encontrados')

			for i in data:
	
				codigo = i.get('codigo')
				status = i.get('status')
				cpfcadastrante = i.get('cpfcadastrante')
				nomecadastrante = i.get('nomecadastrante')
				municipio = i.get('municipio')
				area = i.get('area')
				dataprotocolo = i.get('dataprotocolo')
				dataprotocolo = parse(dataprotocolo)
				tipo = i.get('tipo')
				cpf_cnpj = i.get('cpf_cnpj')
				nome = i.get('nome')
				data_nascimento = i.get('data_nascimento')
				nome_fantasia = i.get('nome_fantasia')
				
				if status:
					
					if status == 'AT':
						status = 'Ativo'
					elif status == 'CA':
						status = 'Cancelado'
					elif status == 'PE':
						status = 'Pendente'

				sql_T(documento,tipo_documento,area,municipio,codigo,dataprotocolo,uf,status)
				xml(documento,tipo_documento,area,municipio,codigo,dataprotocolo,uf,status)

		else:
			logger.debug('Dados não encontrados')
			sql_F(documento, tipo_documento, uf)

	else:
		logger.error("<ERROR: status_code:%s [%s]>"%(r.status_code,documento))
		return


def xml(documento,tipo_documento,area,municipio,codigo,dataprotocolo,uf,status):
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
		u_area_imovel_sicar = area,
		cidade = municipio,
		u_cod_car_sicar = codigo,
		#u_mf_car = mf_car,
		u_status_sicar = status
	)
	logger.debug("<Insert: Registro inserido no ServiceNow [%s]>"%documento)

def sql_T(documento,tipo_documento,area,municipio,codigo,dataprotocolo,uf,status):
	import pyodbc

	if tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"
	elif tipo_documento == "Física":
		tipo_documento = "FISICA"

	try:
		banco = pyodbc.connect('.')
		cursor = banco.cursor()
		cursor.execute("insert into DB_JIVE_2017.dbo.IMOVEIS_CAR(DOCUMENTO_PROPRIETARIO,TIPO_PESSOA,AREA_IMOVEL,CIDADE_IMOVEL,COD_CAR,DATA_CADASTRO_CAR,UF,STATUS_CAR,DATA_INCLUSAO,CRITICA) values((?),(?),(?),(?),(?),(?),(?),(?),getdate(),'Dados capturados com sucesso')", documento,tipo_documento,area,municipio,codigo,dataprotocolo,uf,status)
		banco.commit()
		cursor.close()			
		logger.debug("SQL_T bem sucedido")

	except pyodbc.Error as error:
		
		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("SICAR_MT",error,documento)

	finally:
		banco.close()


def sql_F(documento,tipo_documento,uf):
	import pyodbc

	if tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"
	elif tipo_documento == "Física":
		tipo_documento = "FISICA"

	try:	
		banco = pyodbc.connect('.')
		cursor = banco.cursor()
		cursor.execute("insert into DB_JIVE_2017.dbo.IMOVEIS_CAR(DOCUMENTO_PROPRIETARIO, TIPO_PESSOA, UF, DATA_INCLUSAO, CRITICA) values((?),(?),(?),getdate(),'Não existe cadastro para esta pessoa')", documento,tipo_documento,uf)
		banco.commit()
		cursor.close()			
		logger.debug("SQL_F bem sucedido")

	except pyodbc.Error as error:
		
		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("SICAR_MT",error,documento)

	finally:
		banco.close()



if __name__ == "__main__":

	run('00006167012091','Física')