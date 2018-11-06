import logging
logger = logging.getLogger("Listener.call.TRF3")
########################################################################
def go(documento,tipo_documento,driver):
    log(driver)
    run(documento,tipo_documento,driver)

def log(driver):

    from selenium.webdriver.support.ui import WebDriverWait 
    from selenium.common.exceptions import TimeoutException
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By

    login = False
    while not login:
        try:
            driver.get("http://web.trf3.jus.br/consultas/Internet/ConsultaReqPag")
            WebDriverWait(driver,3).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="formConsulta"]/fieldset""")))
        except (TimeoutException):
            continue

        login = True

def run(documento,tipo_documento,driver):

    from selenium.common.exceptions import NoSuchElementException , TimeoutException , StaleElementReferenceException
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.common.by import By 
    from modules import captcha

    if tipo_documento == "Jurídica":
        logger.debug('Documento validado como Júridico')

        driver.find_element_by_xpath("""//*[@id="CpfCnpj"]""").clear()
        driver.find_element_by_xpath("""//*[@id="CpfCnpj"]""").send_keys(documento)

    elif tipo_documento == "Física":
        logger.debug('Documento validado como Físico')

        driver.find_element_by_xpath("""//*[@id="CpfCnpj"]""").clear()
        driver.find_element_by_xpath("""//*[@id="CpfCnpj"]""").send_keys(documento[3:])

    solved = False
    while not solved:
        ### CAPTCHA SOLVING ###
        img = driver.find_element_by_xpath("""//*[@id="ImagemCaptcha"]""")
        print_screen(img,driver)
        captcha_response = captcha.solve_captcha(r'C:\files\prd\codes\arquivos\captcha.png')  
        driver.find_element_by_xpath("""//*[@id="Captcha"]""").send_keys(captcha_response)
        driver.find_element_by_xpath("""//*[@id="pesquisar"]""").click()

        try:
            error = WebDriverWait(driver,2).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="mensagemErro"]""")))
            error = error.text

            if "Preencha corretamente o código que aparece na imagem" in error:
                continue

            elif "Os dígitos verificadores do CNPJ não conferem." in error:
                logger.debug('Nada localizado')
                sql_F(documento,tipo_documento)

            else:
                logger.error(error)
                print (error)
                return

        except (TimeoutException):
            pass

        solved = True

    try:
        msg = WebDriverWait(driver,60).until(EC.visibility_of_element_located((By.XPATH,"""//*[@id="conteudoPrincipal"]/label""")))
        msg = msg.text
        
        if "Não há ofícios que atendem aos critérios de pesquisa" in msg:
            logger.debug('Nada localizado')
            sql_F(documento,tipo_documento)
            return

        elif "Foram encontradas as seguintes requisições de pagamento" in msg:
            logger.debug('Dados encontrados')
    
            response = driver.find_elements_by_xpath("""//*[@id="conteudoPrincipal"]/table/tbody/tr/td[1]/a""") 
            parse(documento,tipo_documento,driver,response)
        
    except (TimeoutException):
        logger.debug('Timeout')
        return

def parse(documento,tipo_documento,driver,response):
    from datetime import datetime

    links = []
    
    for i in response:
        links.append(i.get_attribute("href"))

    for i in links:
        driver.get(i)

        x = driver.find_elements_by_xpath("""//*[@id="grid"]/tbody/tr/td[1]""")
        y = driver.find_elements_by_xpath("""//*[@id="grid"]/tbody/tr/td[2]""") 

        for a,b in zip(x,y):

            a = a.text
            b = b.text

            if a == "Procedimento":
                procedimento = b
            if a == "Número":
                numero = b  
            if a == "Data prococolo TRF":
                data_protocolo = datetime.strptime(b, '%d/%m/%Y %H:%M:%S')
            if a == "Situação do protocolo":
                situacao_protocolo = b
            if a == "Ofício Requisitório":
                oficio_requisitorio = b
            if a == "Juízo de origem":
                juizo_origem = b
            if a == "Processos originários":
                processos_originarios = b
            if a == "Requerido":
                requerido = b
            if a == "Requerentes":
                requerentes = b
            if a == "Advogado":
                advogado = b
            if a == "M&#234;s/Ano da proposta":
                ano_proposta = b
            if a == "Data conta de liquidação":
                data_liquidacao = datetime.strptime(b, '%d/%m/%Y')
            if a == "Valor solicitado":
                valor_solicitado = b
            if a == "Valor inscrito na proposta":
                valor_inscrito = b
            if a == "Situação da requisição":
                situacao_requisicao = b
            if a == "Banco":
                banco = b
            if a == "Natureza":
                natureza = b

        xml (documento, tipo_documento, oficio_requisitorio, numero, requerido, natureza, situacao_requisicao, valor_solicitado, juizo_origem, data_liquidacao, data_protocolo, situacao_protocolo, procedimento)
        sql_T(documento, tipo_documento, oficio_requisitorio, numero, requerido, natureza, situacao_requisicao, valor_solicitado, juizo_origem, data_liquidacao, data_protocolo, situacao_protocolo, procedimento)
    

def xml(documento, tipo_documento, oficio_requisitorio, numero, requerido, natureza, situacao_requisicao, valor_solicitado, juizo_origem, data_liquidacao, data_protocolo, situacao_protocolo, procedimento):

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
        u_numero_mandado_pagamento_precatorio = oficio_requisitorio,
        u_cod_precat_rio = numero,
        u_devedor_precat_rio = requerido,
        u_natureza_precat_rio = natureza,
        situa_o_precat_rio = situacao_requisicao,
        valor_face_precat_rio = valor_solicitado,
        u_origem_processo_precatorio = juizo_origem,
        u_data_liquida_o_precat_rio = data_liquidacao,
        u_data_protocolo_precat_rio =  data_protocolo,
        u_descri_o_protocolo_precatorio = situacao_protocolo,
        u_tipo_pagamento_precatorio = procedimento,
        u_cr_tica_precatorio = "Sucesso",
        u_fonte_pesquisa_precatorio = "TRF3"
        )
    logger.debug("<Insert: Registro inserido no ServiceNow [%s]>"%documento)


def sql_T(documento, tipo_documento, oficio_requisitorio, numero, requerido, natureza, situacao_requisicao, valor_solicitado, juizo_origem, data_liquidacao, data_protocolo, situacao_protocolo, procedimento):
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
        NUM_MANDADO_PAGTO,
        COD_PRECATORIO,
        DEVEDOR_PRECATORIO,
        NATUREZA_PRECATORIO,
        SITUACAO_PRECATORIO,
        VALOR_FACE_PRECATORIO,
        ORIGEM_PROCESSO,
        DATA_LIQUIDACAO_PRECATORIO,
        DATA_PROTOCOLO_PRECATORIO,
        DESC_PROTOCOLO,
        TIPO_PAGTO,
        DATA_INCLUSAO,
        CRITICA,
        FL_DEVEDOR_JIVE,
        FL_FASE_PRECIFICACAO,
        FONTE_PESQUISA
        ) 
        VALUES ((?),(?),NULL,(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),(?),getdate(),'Dados do Precatorio capturados com Sucesso','N','N','TRF3')""",
        documento, tipo_documento, oficio_requisitorio, numero, requerido, natureza, situacao_requisicao, valor_solicitado, juizo_origem, data_liquidacao, data_protocolo, situacao_protocolo, procedimento)
        banco.commit()
        cursor.close()  
        logger.debug("SQL_T bem sucedido")        

    except pyodbc.Error as error:

        from modules import _email
        _email.send("TRF3",error,documento)

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
        VALUES ((?),(?),NULL,getdate(),'Não existem precatórios para esta pessoa','N','N','TRF3')""",documento,tipo_documento)
        banco.commit()
        cursor.close()  
        logger.debug("SQL_F bem sucedido")        

    except pyodbc.Error as error:

        from modules import _email
        _email.send("TRF3",error,documento)

    finally:
        banco.close()

def print_screen(img,driver):
    from PIL import Image
    
    location = img.location
    size = img.size
    driver.save_screenshot(r'C:\files\prd\codes\arquivos\captcha.png')
    im = Image.open(r'C:\files\prd\codes\arquivos\captcha.png')
    left = location['x']
    top = location['y']
    right = location['x'] + size['width']
    bottom = location['y'] + size['height']
    im = im.crop((left,top,right,bottom))
    im.save(r'C:\files\prd\codes\arquivos\captcha.png')

