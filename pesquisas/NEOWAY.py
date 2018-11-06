import requests,json
from requests import Session
from requests.auth import HTTPBasicAuth
import zeep
from zeep.transports import Transport
from datetime import datetime

################################################

class fisica(object):

	user = "jive-app"
	secret = "XVlBzgbaiC"

	def __init__(self,d):
		self.documento = d
		self.token()
	
	def token(self):

		payload = "{\"application_secret\":\"%s\",\"application\":\"%s\"}"%(self.secret,self.user)

		headers = {
			'content-type': "application/json",
			'cache-control': "no-cache",
			}

		r = requests.post("https://api.neoway.com.br/auth/token", data=payload , headers=headers)

		if r.status_code == 200:

			response = json.loads(r.text)

			if response['token']:

				token = (response['token'])

				self.headers = {"Authorization": "Bearer "+ token}

				return True
			
		return False
		##logger.error

	def consulta(self):

		self.url = "https://api.neoway.com.br/v1/data/pessoas/{0}".format(self.documento[3:])

		nome = self.cadastrais()
		return(nome)
	

	def cadastrais(self):

		wsdl = 'https://jiveasset.service-now.com/x_jam_bd_base_pf_neoway.do?WSDL'
		
		querystring = {"fields":"nome,situacaoCpf,sexo,nomeMae,falecido,dataFalecimento,dataNascimento,cpfMae"}
		r = requests.get(self.url, headers=self.headers, params=querystring)
		if r.status_code == 200:
			data = json.loads(r.text)
			if data:
				nome = data.get('nome',"")
				situacaoCpf = data.get('situacaoCpf',"")
				sexo = data.get('sexo',"")
				dataFalecimento = data.get('dataFalecimento',"")
				dataNascimento = data.get('dataNascimento',"")
				nomeMae = data.get('nomeMae',"")
				cpfMae = data.get('cpfMae',"")
				falecido = data.get('falecido',"")

				if falecido:
					falecido = "Sim"
				else:
					falecido = "Não"

				data = {'u_nome_pessoa_neoway':nome,'u_situa_o_insc_cpf_neoway':situacaoCpf,'u_sexo_neoway':sexo,'u_nome_da_m_e_neoway':nomeMae,'u_ind_co_de_falecimento_neoway':falecido,
				'u_data_falecimento_neoway':dataFalecimento, 'u_data_de_nascimento_neoway':dataNascimento, 'u_cpf_da_m_e_neoway':cpfMae,'u_cpf_neoway':self.documento}

				self.xml(wsdl,data)

				return (nome)


	def xml(self,wsdl,data):
		session = Session()
		session.auth = HTTPBasicAuth(.')
		transport_with_basic_auth = Transport(session=session)

		client = zeep.Client(
			wsdl=wsdl,
			transport=transport_with_basic_auth
		)


		if wsdl == 'https://jiveasset.service-now.com/x_jam_bd_base_pf_neoway.do?WSDL':

			response = client.service.insert(
				u_nome_pessoa_neoway = data['u_nome_pessoa_neoway'],
				u_situa_o_insc_cpf_neoway = data['u_situa_o_insc_cpf_neoway'],
				u_sexo_neoway = data['u_sexo_neoway'],
				u_nome_da_m_e_neoway = data['u_nome_da_m_e_neoway'],
				u_ind_co_de_falecimento_neoway = data['u_ind_co_de_falecimento_neoway'],
				u_data_falecimento_neoway= data['u_data_falecimento_neoway'],
				u_data_de_nascimento_neoway= data['u_data_de_nascimento_neoway'],
				u_cpf_da_m_e_neoway= data['u_cpf_da_m_e_neoway'],
				u_cpf_neoway = data['u_cpf_neoway']
			)

			client = zeep.Client(
				wsdl='https://jiveasset.service-now.com/x_jam_bd_pesquisa.do?WSDL',
				transport=transport_with_basic_auth
			)

			get = client.service.getKeys(
				u_cpf_cnpj= data['u_cpf_neoway']
			)

			count = int(get['count'])

			if count == 1:

				_id = get['sys_id']
							
				update = client.service.update(
					u_cpf_cnpj= data['u_cpf_neoway'],
					u_nome = data['u_nome_pessoa_neoway'],
					u_tipo_pessoa = "Física",
					sys_id=_id[0]
				)

################################################

class juridica(object):

	user = "jive-app"
	secret = "XVlBzgbaiC"

	def __init__(self,d):
		self.documento = d
		self.token()
	
	def token(self):

		payload = "{\"application_secret\":\"%s\",\"application\":\"%s\"}"%(self.secret,self.user)

		headers = {
			'content-type': "application/json",
			'cache-control': "no-cache",
			}

		r = requests.post("https://api.neoway.com.br/auth/token", data=payload , headers=headers)

		if r.status_code == 200:

			response = json.loads(r.text)

			if response['token']:

				token = (response['token'])

				self.headers = {"Authorization": "Bearer "+ token}

				return True
			
		logger.error('<ERROR: Request status_code (%s) [%s]>'%(r.status_code,self.documento))
		return False
		##logger.error

	def consulta(self):

		self.url = "https://api.neoway.com.br/v1/data/empresas/{0}".format(self.documento)

		nome = self.cadastrais()
		self.antt()
		self.arts()
		#self.aeronaves()
		#self.beneficiarios()
		#self.beneficiariosJunta()
		#self.beneficiariosQsaUnificado()
		#self.ceis()
		#self.cepim()
		#self.cnaePrincipal()
		self.empresasColigadas()
		self.funcionarios()
		self.qsaUnificado()
		self.socios()
		self.sociosJunta()
		self.veiculos()

		return nome
		
	##################################################################################
		
	def aeronaves(self):

		querystring = {"fields":"aeronaves.ano,aeronaves.fabricante,aeronaves.matricula,aeronaves.modelo,aeronaves.operador,aeronaves.proprietario"}
		r = requests.get(self.url, headers=self.headers, params=querystring)
		print(r.status_code)
		if r.status_code == 200:
			data = (json.loads(r.text))
			params = data.get('aeronaves')
			if params:
				for i in params:
					aeronaves_ano = i.get('ano')
					aeronaves_fabricante = i.get('fabricante')
					aeronaves_matricula = i.get('matricula')
					aeronaves_modelo = i.get('modelo')
					aeronaves_operador = i.get('operador')
					aeronaves_proprietario = i.get('proprietario')

					print(
					aeronaves_ano,
					aeronaves_fabricante,
					aeronaves_matricula,
					aeronaves_modelo,
					aeronaves_operador,
					aeronaves_proprietario
					)

	def antt(self):

		wsdl = 'https://jiveasset.service-now.com/x_jam_bd_antt.do?WSDL'

		querystring = {"fields":"antt.cnpj,antt.dataAtualizacao,antt.dataValidade,antt.modalidade,antt.numeroCertificado,antt.regime"}
		r = requests.get(self.url, headers=self.headers, params=querystring)
		if r.status_code == 200:
			data = (json.loads(r.text))
			params = data.get('antt')
			if params:
				for i in params:
					antt_cnpj = i.get('cnpj')
					antt_dataAtualizacao = i.get('dataAtualizacao')
					antt_dataValidade = i.get('dataValidade')
					antt_modalidade = i.get('modalidade')
					antt_numeroCertificado = i.get('numeroCertificado')
					antt_regime = i.get('regime')

					if antt_dataAtualizacao:
						antt_dataAtualizacao = datetime.strptime(antt_dataAtualizacao[:10],'%Y-%m-%d').date().strftime('%d/%m/%Y')
					if antt_dataValidade:
						antt_dataValidade = datetime.strptime(antt_dataValidade[:10],'%Y-%m-%d').date().strftime('%d/%m/%Y')

					data = {'u_cpf_cnpj':self.documento, 'u_data_atualizacao_antt':antt_dataAtualizacao, 'u_data_validade_do_certificado':antt_dataValidade, 'u_modalidade_de_transporte':antt_modalidade,'u_n_crf':antt_numeroCertificado,'u_regime_de_transporte':antt_regime}
					
					self.xml(wsdl,data)


	def arts(self):

		wsdl = 'https://jiveasset.service-now.com/x_jam_bd_arts.do?WSDL'

		querystring = {"fields":"arts.bairro,arts.cep,arts.cnpj,arts.dataInicio,arts.dataPrevisaoTermino,arts.dataRegistro,arts.finalidade.desc,arts.municipio,arts.numeroArt,arts.observacao,arts.quantidadeObrasServicos,arts.uf,arts.valorTotalServico,arts.vinculo"}
		r = requests.get(self.url, headers=self.headers, params=querystring)
		if r.status_code == 200:
			data = (json.loads(r.text))
			params = data.get('arts')
			if params:
				for i in params:
					arts_bairro = i.get('bairro')
					arts_cep = i.get('cep')
					arts_cnpj = i.get('cnpj')
					arts_dataInicio = i.get('dataInicio')
					arts_dataPrevisaoTermino = i.get('dataPrevisaoTermino')
					arts_dataRegistro =  i.get('dataRegistro')
					arts_finalidade_desc = i.get('finalidade')
					arts_municipio = i.get('municipio')
					arts_numeroArt = i.get('numeroArt')
					arts_observacao = i.get('observacao')
					arts_quantidadeObrasServicos = i.get('quantidadeObrasServicos')
					arts_uf = i.get('uf')
					arts_valorTotalServico = i.get('valorTotalServico')
					arts_vinculo = i.get('vinculo')

					if arts_dataInicio:
						arts_dataInicio = datetime.strptime(arts_dataInicio[:10],'%Y-%m-%d').date().strftime('%d/%m/%Y')
					if arts_dataPrevisaoTermino:
						arts_dataPrevisaoTermino = datetime.strptime(arts_dataPrevisaoTermino[:10],'%Y-%m-%d').date().strftime('%d/%m/%Y')
					if arts_dataRegistro:
						arts_dataRegistro = datetime.strptime(arts_dataRegistro[:10],'%Y-%m-%d').date().strftime('%d/%m/%Y')

					data = {'u_cpf_cnpj':self.documento,'u_bairro':arts_bairro, 'u_cep':arts_cep, 'u_data_de_inicio':arts_dataInicio, 'previsao_termino':arts_dataPrevisaoTermino, 'u_data_de_registro':arts_dataRegistro, 'n_art':arts_numeroArt,
					'u_finalidade':arts_finalidade_desc, 'u_municipio':arts_municipio, 'u_observacao':arts_observacao, 'u_quantidade_de_obra_servico':arts_quantidadeObrasServicos, 'u_uf':arts_uf, 'u_valor':arts_valorTotalServico, 'u_vinculo_art':arts_vinculo}

					self.xml(wsdl,data)


	def beneficiarios(self):

		querystring = {"fields":"beneficiarios.documento,beneficiarios.falecido,beneficiarios.grau,beneficiarios.participacao"}
		r = requests.get(self.url, headers=self.headers, params=querystring)
		print(r.text)
		if r.status_code == 200:
			data = (json.loads(r.text))
			params = data.get('beneficiarios')
			if params:
				for i in params:
					beneficiarios_documento = i.get('documento')
					beneficiarios_falecido = i.get('falecido')
					beneficiarios_grau = i.get('grau')
					beneficiarios_participacao = i.get('participacao')

					print(
					beneficiarios_documento,
					beneficiarios_falecido,
					beneficiarios_grau,
					beneficiarios_participacao
					)


	def beneficiariosJunta(self):

		querystring = {"fields":"beneficiariosJunta.documento,beneficiariosJunta.falecido,beneficiariosJunta.grau,beneficiariosJunta.participacao"}
		r = requests.get(self.url, headers=self.headers, params=querystring)
		print(r.text)
		if r.status_code == 200:
			data = (json.loads(r.text))
			params = data.get('beneficiariosJunta')
			if params:
				for i in params:
					beneficiariosJunta_documento = i.get('documento')
					beneficiariosJunta_falecido = i.get('falecido')
					beneficiariosJunta_grau = i.get('grau')
					beneficiariosJunta_participacao = i.get('participacao')

					print(
					beneficiariosJunta_documento,
					beneficiariosJunta_falecido,
					beneficiariosJunta_grau,
					beneficiariosJunta_participacao
					)


	def beneficiariosQsaUnificado(self):

		querystring = {"fields":"beneficiariosQsaUnificado.documento,beneficiariosQsaUnificado.falecido,beneficiariosQsaUnificado.grau,beneficiariosQsaUnificado.participacao"}
		r = requests.get(self.url, headers=self.headers, params=querystring)
		print(r.text)
		if r.status_code == 200:
			data = (json.loads(r.text))
			params = data.get('beneficiariosQsaUnificado')
			if params:
				for i in params:
					beneficiariosQsaUnificado_documento = i.get('documento')
					beneficiariosQsaUnificado_falecido = i.get('falecido')
					beneficiariosQsaUnificado_grau = i.get('grau')
					beneficiariosQsaUnificado_participacao = i.get('participacao')

					print(
					beneficiariosQsaUnificado_documento,
					beneficiariosQsaUnificado_falecido,
					beneficiariosQsaUnificado_grau,
					beneficiariosQsaUnificado_participacao
					)

	def cadastrais(self):

		wsdl = 'https://jiveasset.service-now.com/x_jam_bd_base_pj_neoway.do?WSDL'

		querystring = {"fields":"situacao.descricao,cnaePrincipal.setor,razaoSocial,cnaePrincipal.ramoAtividade,matriz.quantidadeFilial,fantasia,matriz.empresaMatriz,cnaePrincipal.codigo,natureza.descricao,cnaePrincipal.descricao,situacao.data,dataAbertura,natureza.id,natureza.classificacao"}
		r = requests.get(self.url, headers=self.headers, params=querystring)
		if r.status_code == 200:
			data = json.loads(r.text)
			if data:
				matriz_situacao = data.get('situacao',"").get('descricao',"")
				matriz_quantidade = data.get('matriz',"").get('quantidadeFilial',"")
				razaoSocial = data.get('razaoSocial',"")
				fantasia = data.get('fantasia',"")
				situacao_data = data.get('situacao',"").get('data',"")
				dataAbertura = data.get('dataAbertura',"")
				natureza = data.get('natureza',"").get('descricao',"")
				natureza_id = data.get('natureza',"").get('id',"")
				natureza_classificacao = data.get('natureza',"").get('classificacao',"")
				matriz = data.get('matriz',"").get('empresaMatriz',"")

				if matriz:
					matriz = "Sim"
				else:
					matriz = "Não"
				
				if data.get('cnaePrincipal'):
					cnaePrincipal_setor = data.get('cnaePrincipal',"").get('setor',"")
					cnaePrincipal_ramo = data.get('cnaePrincipal',"").get('ramoAtividade',"")
					cnaePrincipal_codigo = data.get('cnaePrincipal',"").get('codigo',"")
					cnaePrincipal_descricao = data.get('cnaePrincipal',"").get('descricao',"")
				else:
					cnaePrincipal_setor = ' '
					cnaePrincipal_ramo = ' '
					cnaePrincipal_codigo = ' '
					cnaePrincipal_descricao = ' '


				data = {'u_cnpj_neoway':self.documento, 'u_situa_o_cadastral_neoway':matriz_situacao, 'u_setor_neoway':cnaePrincipal_setor, 'u_raz_o_social_neoway':razaoSocial, 'u_ramo_de_atividade_neoway':cnaePrincipal_ramo, 'u_n_mero_de_filiais_ativas_neoway':matriz_quantidade,
				'u_nome_fantasia_neoway':fantasia,'u_matriz_neoway':matriz,'u_identificador_do_cnae_principal_neoway':cnaePrincipal_codigo,'u_descri_o_natureza_jur_dica_neoway':natureza, 'u_descri_o_do_cnae_principal_neoway':cnaePrincipal_descricao,
				'u_data_situa_o_cadastral_neoway':situacao_data, 'u_data_abertura_neoway':dataAbertura, 'u_c_digo_natureza_jur_dica_neoway':natureza_id, 'u_classifica_o_natureza_jur_dica_neoway':natureza_classificacao}

				self.xml(wsdl,data)

				return (razaoSocial)


	def ceis(self):

		querystring = {"fields":"ceis.dataFimSancao,ceis.dataInicioSancao,ceis.fundamentacaoLegal,ceis.identificador,ceis.numeroProcesso,ceis.tipoSancao"}
		r = requests.get(self.url, headers=self.headers, params=querystring)
		if r.status_code == 200:
			data = (json.loads(r.text))
			params = data.get('ceis')
			if params:
				for i in params:
					ceis_dataFimSancao = i.get('dataFimSancao')
					ceis_dataInicioSancao = i.get('dataInicioSancao')
					ceis_fundamentacaoLegal = i.get('fundamentacaoLegal')
					ceis_identificador = i.get('identificador')
					ceis_numeroProcesso = i.get('numeroProcesso')
					ceis_tipoSancao = i.get('tipoSancao')

					print(
					ceis_dataFimSancao,
					ceis_dataInicioSancao,
					ceis_fundamentacaoLegal,
					ceis_identificador,
					ceis_numeroProcesso,
					ceis_tipoSancao
					)

	def cepim(self):

		querystring = {"fields":"cepim.cnpj,cepim.convenio,cepim.identificador,cepim.motivo.raw,cepim.orgaoConcedente"}
		r = requests.get(self.url, headers=self.headers, params=querystring)
		print(r.text)
		if r.status_code == 200:
			data = (json.loads(r.text))
			params = data.get('cepim')
			if params:
				for i in params:
					cepim_cnpj = i.get('cnpj')
					cepim_convenio = i.get('convenio')
					cepim_identificador = i.get('identificador')
					cepim_motivo_raw = i.get('motivo.raw')
					cepim_orgaoConcedente = i.get('orgaoConcedente')
					
					print(
					cepim_cnpj,
					cepim_convenio,
					cepim_identificador,
					cepim_motivo_raw,
					cepim_orgaoConcedente
					)


	def cfc(self):

		querystring = {"fields":"cfc.codigoCFC,cfc.registro,cfc.situacao,cfc.tipoConsulta,cfc.tipoRegistro"}
		r = requests.get(self.url, headers=self.headers, params=querystring)
		print(r.text)
		if r.status_code == 200:
			data = (json.loads(r.text))
			params = data.get('cfc')
			if params:
				for i in params:
					cfc_codigoCFC = i.get('codigoCFC')
					cfc_registro = i.get('registro')
					cfc_situacao = i.get('situacao')
					cfc_tipoConsulta = i.get('tipoConsulta')
					cfc_tipoRegistro = i.get('tipoRegistro')
					
					print(
					cepim_cnpj,
					cepim_convenio,
					cepim_identificador,
					cepim_motivo_raw,
					cepim_orgaoConcedente
					)


	def cnaePrincipal(self):

		querystring = {"fields":"cnaePrincipal.codigo,cnaePrincipal.descricao,cnaePrincipal.ramoAtividade,cnaePrincipal.setor"}
		r = requests.get(self.url, headers=self.headers, params=querystring)
		print(r.text)
		if r.status_code == 200:
			data = (json.loads(r.text))
			i = data.get('cnaePrincipal')

			cnaePrincipal_codigo = i.get('codigo')
			cnaePrincipal_descricao = i.get('descricao')
			cnaePrincipal_ramoAtividade = i.get('ramoAtividade')
			cnaePrincipal_setor = i.get('setor')

			print(
			cnaePrincipal_codigo,
			cnaePrincipal_descricao,
			cnaePrincipal_ramoAtividade,
			cnaePrincipal_setor
			)


	def empresasColigadas(self):

		wsdl = 'https://jiveasset.service-now.com/x_jam_bd_empresas_coligadas_neoway.do?WSDL'
		
		querystring = {"fields":"empresasColigadas.cnpj,empresasColigadas.dataAbertura,empresasColigadas.municipio,empresasColigadas.razaoSocial,empresasColigadas.uf"}
		r = requests.get(self.url, headers=self.headers, params=querystring)
		if r.status_code == 200:
			data = (json.loads(r.text))
			params = data.get('empresasColigadas')
			if params:
				for i in params:
					empresasColigadas_cnpj = i.get('cnpj')
					empresasColigadas_dataAbertura = i.get('dataAbertura')
					empresasColigadas_municipio = i.get('municipio')
					empresasColigadas_razaoSocial = i.get('razaoSocial')
					empresasColigadas_uf = i.get('uf')

					if empresasColigadas_dataAbertura:
						empresasColigadas_dataAbertura = datetime.strptime(empresasColigadas_dataAbertura[:10],'%Y-%m-%d').date().strftime('%d/%m/%Y')

					data = {'u_cpf_cnpj':self.documento, 'u_cnpj_coligada':empresasColigadas_cnpj, 'u_data_abertura':empresasColigadas_dataAbertura, 'u_municipio':empresasColigadas_municipio, 'u_razao_social':empresasColigadas_razaoSocial, 'uf':empresasColigadas_uf}

					self.xml(wsdl,data)


	def funcionarios(self):

		wsdl = 'https://jiveasset.service-now.com/x_jam_bd_funcionarios_neoway.do?WSDL'

		querystring = {"fields":"funcionarios.cpf,funcionarios.dataAdmissao,funcionarios.dataNascimento,funcionarios.nome"}
		r = requests.get(self.url, headers=self.headers, params=querystring)
		if r.status_code == 200:
			data = (json.loads(r.text))
			params = data.get('funcionarios')
			if params:
				for i in params:
					funcionarios_cpf = i.get('cpf')
					funcionarios_dataAdmissao = i.get('dataAdmissao')
					funcionarios_dataNascimento = i.get('dataNascimento')			
					funcionarios_nome = i.get('nome')
					
					if funcionarios_dataAdmissao:
						funcionarios_dataAdmissao = datetime.strptime(funcionarios_dataAdmissao[:10],'%Y-%m-%d').date().strftime('%d/%m/%Y')
					if funcionarios_dataNascimento:
						funcionarios_dataNascimento = datetime.strptime(funcionarios_dataNascimento[:10],'%Y-%m-%d').date().strftime('%d/%m/%Y')

					data = {'u_cpf_cnpj':self.documento,'u_cpf_do_funcionario':funcionarios_cpf,'u_data_de_adminissao':funcionarios_dataAdmissao,'u_data_de_nascimento':funcionarios_dataNascimento,'u_nome_do_funcionario':funcionarios_nome}

					self.xml(wsdl,data)


	def qsaUnificado(self):

			wsdl = 'https://jiveasset.service-now.com/x_jam_bd_socios_qsa_neoway.do?WSDL'

			querystring = {"fields":"qsaUnificado.documento,qsaUnificado.nome,qsaUnificado.falecido,qsaUnificado.paisOrigem,qsaUnificado.nivelPep"}
			r = requests.get(self.url, headers=self.headers, params=querystring)
			if r.status_code == 200:
				data = (json.loads(r.text))
				params = data.get('qsaUnificado')
				if params:
					for i in params:
						qsaUnificado_documento = i.get('documento')
						qsaUnificado_falecido = i.get('falecido')
						qsaUnificado_nivelPep = i.get('nivelPep')
						qsaUnificado_nome = i.get('nome')
						qsaUnificado_paisOrigem = i.get('paisOrigem')

						if qsaUnificado_falecido:
							qsaUnificado_falecido = 'Sim'
						else:
							qsaUnificado_falecido = 'Não'

						data = {'u_cpf_cnpj':self.documento,'u_cpf_cnpj_do_socio':qsaUnificado_documento,'u_registro_de_obito':qsaUnificado_falecido,'u_nivel_pep':qsaUnificado_nivelPep,'u_nome_razao_social_do_socio':qsaUnificado_nome,'u_pais_de_origem':qsaUnificado_paisOrigem}

						self.xml(wsdl,data)


	def socios (self):

		wsdl = 'https://jiveasset.service-now.com/x_jam_bd_socios_neoway.do?WSDL'

		querystring = {"fields":"socios.documento,socios.falecido,socios.nivelPep,socios.nome,socios.paisOrigem"}
		r = requests.get(self.url, headers=self.headers, params=querystring)
		if r.status_code == 200:
			data = (json.loads(r.text))
			params = data.get('socios')
			if params:
				for i in params:
					socios_documento = i.get('documento')
					socios_falecido = i.get('falecido')
					socios_nivelPep = i.get('nivelPep')
					socios_nome = i.get('nome')
					socios_paisOrigem = i.get('paisOrigem')

					if socios_falecido:
						socios_falecido = 'Sim'
					else:
						socios_falecido = 'Não'
			
					data = {'u_cpf_cnpj':self.documento,'u_cpf_cnpj_do_socio':socios_documento,'u_registro_de_obito':socios_falecido,'u_nivel_pep':socios_nivelPep,'u_nome_razao_social_do_socio':socios_nome,'u_pais_de_origem':socios_paisOrigem}

					self.xml(wsdl,data)


	def sociosJunta (self):

		wsdl = 'https://jiveasset.service-now.com/x_jam_bd_socios_junta_neoway.do?WSDL'

		querystring = {"fields":"sociosJunta.documento,sociosJunta.falecido,sociosJunta.nivelPep,sociosJunta.nome"}
		r = requests.get(self.url, headers=self.headers, params=querystring)
		if r.status_code == 200:
			data = (json.loads(r.text))
			params = data.get('sociosJunta')
			if params:
				for i in params:
					sociosJunta_documento = i.get('documento')
					sociosJunta_falecido = i.get('falecido')
					sociosJunta_nivelPep = i.get('nivelPep')
					sociosJunta_nome = i.get('nome')

					if sociosJunta_falecido:
						sociosJunta_falecido = 'Sim'
					else:
						sociosJunta_falecido = 'Não'
			
					data = {'u_cpf_cnpj':self.documento,'u_cpf_cnpj_do_socio':sociosJunta_documento,'u_registro_de_obito':sociosJunta_falecido,'u_nivel_pep':sociosJunta_nivelPep,'u_nome_razao_social_do_socio':sociosJunta_nome}

					self.xml(wsdl,data)


	def veiculos(self):

		wsdl = 'https://jiveasset.service-now.com/x_jam_bd_veiculos_neoway.do?WSDL'

		querystring = {"fields":"veiculos.anoFabricacao,veiculos.classificacao,veiculos.combustivel,veiculos.constaAntt,veiculos.marcaModelo.desc,veiculos.placa,veiculos.tipo,veiculos.uf"}
		r = requests.get(self.url, headers=self.headers, params=querystring)
		if r.status_code == 200:
			data = (json.loads(r.text))
			params = data.get('veiculos')
			if params:
				for i in params:
					veiculos_anoFabricacao = i.get('anoFabricacao')
					veiculos_classificacao = i.get('classificacao')
					veiculos_combustivel = i.get('combustivel')
					veiculos_constaAntt = i.get('constaAntt')
					veiculos_marcaModelo_desc = i.get('marcaModelo')
					veiculos_placa = i.get('placa')
					veiculos_tipo = i.get('tipo')
					veiculos_uf = i.get('uf')

					data =  {'u_cpf_cnpj':self.documento, 'u_ano_de_fabricacao':veiculos_anoFabricacao, 'u_classificacao':veiculos_classificacao, 'u_combustivel':veiculos_combustivel, 'u_consta_na_antt':veiculos_constaAntt, 'u_marca_modelo':veiculos_marcaModelo_desc, 'u_placa':veiculos_placa, 'u_tipo_veiculo':veiculos_tipo, 'u_uf':veiculos_uf}

					self.xml(wsdl,data)	


	##################################################################################	

	def xml(self,wsdl,data):
		session = Session()
		session.auth = HTTPBasicAuth(.')
		transport_with_basic_auth = Transport(session=session)

		client = zeep.Client(
		wsdl=wsdl,
		transport=transport_with_basic_auth
		)

		#antt

		if wsdl == 'https://jiveasset.service-now.com/x_jam_bd_antt.do?WSDL':

			response = client.service.insert(
				u_cpf_cnpj = data['u_cpf_cnpj'],
				u_data_atualizacao_antt = data['u_data_atualizacao_antt'],
				u_data_validade_do_certificado = data['u_data_validade_do_certificado'],
				u_modalidade_de_transporte = data['u_modalidade_de_transporte'],
				u_n_crf = data['u_n_crf'],
				u_regime_de_transporte = data['u_regime_de_transporte']
			)

			

		#arts	

		if wsdl == 'https://jiveasset.service-now.com/x_jam_bd_arts.do?WSDL':

			response = client.service.insert(
				n_art = data['n_art'],
				previsao_termino = data['previsao_termino'],
				u_bairro = data['u_bairro'],
				u_cep = data['u_cep'],
				u_cpf_cnpj = data['u_cpf_cnpj'],
				u_data_de_inicio = data['u_data_de_inicio'],
				u_data_de_registro = data['u_data_de_registro'],
				u_finalidade = data['u_finalidade'],
				u_municipio = data['u_municipio'],
				u_observacao = data['u_observacao'],
				u_quantidade_de_obra_servico = data['u_quantidade_de_obra_servico'],
				u_uf = data['u_uf'],
				u_valor = data['u_valor'],
				u_vinculo_art = data['u_vinculo_art']
			)


		#cadastrais

		if wsdl == 'https://jiveasset.service-now.com/x_jam_bd_base_pj_neoway.do?WSDL':

			response = client.service.insert(
				u_cnpj_neoway = data['u_cnpj_neoway'],
				u_situa_o_cadastral_neoway =  data['u_situa_o_cadastral_neoway'],
				u_setor_neoway =  data['u_setor_neoway'],
				u_raz_o_social_neoway =  data['u_raz_o_social_neoway'],
				u_ramo_de_atividade_neoway =  data['u_ramo_de_atividade_neoway'],
				u_n_mero_de_filiais_ativas_neoway =  data['u_n_mero_de_filiais_ativas_neoway'],
				u_nome_fantasia_neoway =  data['u_nome_fantasia_neoway'],
				u_matriz_neoway =  data['u_matriz_neoway'],
				u_identificador_do_cnae_principal_neoway =  data['u_identificador_do_cnae_principal_neoway'],
				u_descri_o_natureza_jur_dica_neoway =  data['u_descri_o_natureza_jur_dica_neoway'],
				u_descri_o_do_cnae_principal_neoway =  data['u_descri_o_do_cnae_principal_neoway'],
				u_data_situa_o_cadastral_neoway =  data['u_data_situa_o_cadastral_neoway'],
				u_data_abertura_neoway =  data['u_data_abertura_neoway'],
				u_c_digo_natureza_jur_dica_neoway =  data['u_c_digo_natureza_jur_dica_neoway'],
				u_classifica_o_natureza_jur_dica_neoway =  data['u_classifica_o_natureza_jur_dica_neoway']
			)

			client = zeep.Client(
				wsdl='https://jiveasset.service-now.com/x_jam_bd_pesquisa.do?WSDL',
				transport=transport_with_basic_auth
			)

			get = client.service.getKeys(
				u_cpf_cnpj= self.documento
			)

			count = int(get['count'])

			if count == 1:

				_id = get['sys_id']
							
				update = client.service.update(
					u_cpf_cnpj= data['u_cnpj_neoway'],
					u_nome = data['u_raz_o_social_neoway'],
					u_tipo_pessoa = "Jurídica",
					sys_id=_id[0]
				)

		#empresasColigadas

		if wsdl == 'https://jiveasset.service-now.com/x_jam_bd_empresas_coligadas_neoway.do?WSDL':

			response = client.service.insert(
				u_cpf_cnpj = data['u_cpf_cnpj'],
				#u_cnae_principal = data[''],
				u_cnpj_coligada = data['u_cnpj_coligada'],
				u_data_abertura = data['u_data_abertura'],
				u_municipio = data['u_municipio'],
				u_razao_social = data['u_razao_social'],
				uf = data['uf']
			)

		if wsdl == 'https://jiveasset.service-now.com/x_jam_bd_funcionarios_neoway.do?WSDL':

			response = client.service.insert(
				u_cpf_cnpj = data['u_cpf_cnpj'],
				u_cpf_do_funcionario = data['u_cpf_do_funcionario'],
				u_data_de_adminissao = data['u_data_de_adminissao'],
				u_data_de_nascimento = data['u_data_de_nascimento'],
				u_nome_do_funcionario = data['u_nome_do_funcionario']
			)


		if wsdl == 'https://jiveasset.service-now.com/x_jam_bd_socios_neoway.do?WSDL':

			response = client.service.insert(
				u_cpf_cnpj = data['u_cpf_cnpj'],
				u_cpf_cnpj_do_socio = data['u_cpf_cnpj_do_socio'],
				u_nivel_pep = data['u_nivel_pep'],
				u_nome_razao_social_do_socio = data['u_nome_razao_social_do_socio'],
				u_pais_de_origem = data['u_pais_de_origem'],
				u_registro_de_obito = data['u_registro_de_obito']
				#u_participacao_societaria = data['u_participacao_societaria'],
				#u_qualificacao = data['u_qualificacao']
			)



		if wsdl == 'https://jiveasset.service-now.com/x_jam_bd_socios_junta_neoway.do?WSDL':

			response = client.service.insert(
				u_cpf_cnpj = data['u_cpf_cnpj'],
				u_cpf_cnpj_do_socio = data['u_cpf_cnpj_do_socio'],
				u_nivel_pep = data['u_nivel_pep'],
				u_nome_razao_social_do_socio = data['u_nome_razao_social_do_socio'],
				u_registro_de_obito = data['u_registro_de_obito']
			) 



		if wsdl == 'https://jiveasset.service-now.com/x_jam_bd_socios_qsa_neoway.do?WSDL':

			response = client.service.insert(
				u_cpf_cnpj = data['u_cpf_cnpj'],
				u_cpf_cnpj_do_socio = data['u_cpf_cnpj_do_socio'],
				u_nivel_pep = data['u_nivel_pep'],
				u_nome_razao_social_do_socio = data['u_nome_razao_social_do_socio'],
				u_pais_de_origem = data['u_pais_de_origem'],
				u_registro_de_obito = data['u_registro_de_obito']
				#u_participacao_societaria = data['u_participacao_societaria'],
				#u_qualificacao = data['u_qualificacao']
			)



		if wsdl == 'https://jiveasset.service-now.com/x_jam_bd_veiculos_neoway.do?WSDL':

			response = client.service.insert(
				u_cpf_cnpj = data['u_cpf_cnpj'],
				u_ano_de_fabricacao = data['u_ano_de_fabricacao'],
				u_classificacao = data['u_classificacao'],
				u_combustivel = data['u_combustivel'],
				u_consta_na_antt = data['u_consta_na_antt'],
				u_marca_modelo = data['u_marca_modelo'],
				u_placa = data['u_placa'],
				u_tipo_veiculo = data['u_tipo_veiculo'],
				u_uf = data['u_uf']
			)	



if __name__ == '__main__':

	neoway = juridica('47508411022559')
	neoway.consulta()

