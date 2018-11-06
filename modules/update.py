import logging
logger = logging.getLogger("Listener.modules.update")

def atualizar(documento,tipo_documento):

	from requests import Session
	from requests.auth import HTTPBasicAuth
	import zeep
	from zeep.transports import Transport
	
	session = Session()
	session.auth = HTTPBasicAuth('example', 'example')
	transport_with_basic_auth = Transport(session=session)

	client = zeep.Client(
		wsdl='https://jiveasset.service-now.com/x_jam_bd_pesquisa.do?WSDL',
		transport=transport_with_basic_auth
	)

	get = client.service.getKeys(
		u_cpf_cnpj= documento
	)
	
	count = int(get['count'])

	if count == 1:

		_id = get['sys_id']
						
		update = client.service.update(
			state="2",
			u_cpf_cnpj= documento,
			u_tipo_pessoa = tipo_documento,
			sys_id=_id[0]
		)
		logger.info("<Update: Tabela de pesquisas do ServiceNow atualizada [%s]>"%documento)

	else:
		logger.error("<ERROR: Documento não existe ou duplicado na tabela de pesquisas [%s]>"%documento)
		return 
