import requests,json, logging

logger = logging.getLogger("Listener.call.MIAMIDADE")

def run(documento,tipo_documento,nome):

	headers = {"Referer":"http://www.miamidade.gov/propertysearch/"}

	r = requests.get("http://www.miamidade.gov/PApublicServiceProxy/PaServicesProxy.ashx?Operation=GetOwners&clientAppName=PropertySearch&enPoint=&from=1&ownerName={0}&to=100".format(nome), headers=headers ,timeout=60)

	if r.status_code == 200:

		j = json.loads(r.text)

		propertys = j.get('MinimumPropertyInfos')

		if propertys:

			checked = False
			for i in propertys:

				municipality = i.get('Municipality')
				neighborhood_description = i.get('NeighborhoodDescription')
				owner1 = i.get('Owner1')
				owner2 = i.get('Owner2')
				owner3 = i.get('Owner3')
				site_address = i.get('SiteAddress')
				site_unit = i.get('SiteUnit')
				status = i.get('Status')
				property_strap = i.get('Strap')
				subdivision_description = i.get('SubdivisionDescription')

				if nome == owner1 or nome == owner2 or nome == owner3:

					if checked:
						pass
					else:
						checked = True

					sql_T(documento,tipo_documento,nome,municipality,neighborhood_description,owner1,owner2,owner3,site_address,site_unit,status,property_strap,subdivision_description)
					#xml(documento,municipality,neighborhood_description,owner1,owner2,owner3,site_address,site_unit,status,property_strap,subdivision_description)

			if not checked:
				logger.debug("Nada localizado")
				sql_F(documento,tipo_documento,nome)

		else:
			logger.debug("Nada localizado")
			sql_F(documento,tipo_documento,nome)

	else:
		logger.error("<ERROR: status_code:%s [%s]>"%(r.status_code,documento))
		return


def sql_T(documento,tipo_documento,nome,municipality,neighborhood_description,owner1,owner2,owner3,site_address,site_unit,status,property_strap,subdivision_description):

	import pyodbc

	if tipo_documento == "Física":
		tipo_documento = "FISICA"
	elif tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"
	
	try:
		banco = pyodbc.connect('.')
		cursor = banco.cursor()
		cursor.execute("""INSERT INTO DB_JIVE_2017.dbo.MIAMIDADE(
		DOCUMENTO_PESSOA,TIPO_PESSOA,NOME_PESSOA, MUNICIPALITY, NEIGHBORHOOD_DESCRIPTION, OWNER1, OWNER2, OWNER3, SITE_ADDRESS, SITE_UNIT, STATUS, PROPERTY_STRAP, SUBDIVISION_DESCRIPTION, DATA_INCLUSAO, CRITICA)
		VALUES((?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),getdate(),'Sucesso')""",documento,tipo_documento,nome,municipality,neighborhood_description,owner1,owner2,owner3,site_address,site_unit,status,property_strap,subdivision_description)
		banco.commit()
		cursor.close()			
		logger.debug("SQL_T bem sucedido")

	except pyodbc.Error as error:
		
		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("MIAMIDADE",error,documento)

	finally:
		banco.close()

def sql_F(documento,tipo_documento,nome):

	import pyodbc

	if tipo_documento == "Física":
		tipo_documento = "FISICA"
	elif tipo_documento == "Jurídica":
		tipo_documento = "JURIDICA"

	try:
		banco = pyodbc.connect('.')
		cursor = banco.cursor()
		cursor.execute("INSERT INTO DB_JIVE_2017.dbo.MIAMIDADE(DOCUMENTO_PESSOA,TIPO_PESSOA,NOME_PESSOA,DATA_INCLUSAO,CRITICA) VALUES((?),(?),(?),getdate(),'Nada localizado')", documento,tipo_documento,nome)
		banco.commit()
		cursor.close()			
		logger.debug("SQL_F bem sucedido")

	except pyodbc.Error as error:
		
		from modules import _email
		logger.error("<ERROR: %s [%s]>"%(error, documento))
		_email.send("MIAMIDADE",error,documento)

	finally:
		banco.close()

def xml(documento,municipality,neighborhood_description,owner1,owner2,owner3,site_address,site_unit,status,property_strap,subdivision_description):

		from requests import Session
		from requests.auth import HTTPBasicAuth
		import zeep
		from zeep.transports import Transport

		session = Session()
		session.auth = HTTPBasicAuth(.')
		transport_with_basic_auth = Transport(session=session)

		client = zeep.Client(
			wsdl='https://jiveasset.service-now.com/x_jam_bd_miamidade.do?WSDL',
			transport=transport_with_basic_auth
		)

		response = client.service.insert(
			u_cpf_cnpj = documento,
			u_municipality = municipality,
			u_neighborhood_description = neighborhood_description,
			u_owner_1 = owner1,
			u_owner_2 = owner2,
			u_owner_3 = owner3,
			u_site_adress = site_address,
			u_site_unit = site_unit,
			u_status = status,
			u_property_strap = property_strap,
			u_subdivision_description = subdivision_description
		)
		
		logger.debug("<Insert: Registro inserido no ServiceNow [%s]>"%documento)


