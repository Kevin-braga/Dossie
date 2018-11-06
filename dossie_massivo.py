__author__ = "Kevin Braga"
__copyright__ = "Copyright 2017, Jive Investments"
__credits__ = ["Felipe Benevides", "Evandro Almeida", "Allan Nicacio"]
__license__ = "GPL"
__version__ = "1.0.5"
__maintainer__ = "Nakamura"
__email__ = "kevin-dkno@hotmail.com"
__status__ = "Production"

###################################
from PIL import ImageTk
from tkinter import filedialog
import tkinter.messagebox
from tkinter import *
import time , shutil, os.path, logging , sys, os, requests, logging
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from threading import Thread
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from modules import *
from pesquisas import *
from pesquisas import BETHA_M
import pyodbc

##################################################################		

class Menu(object):
	def __init__(self):

		self.root = Tk()
		self.root.title("Massiva")
		self.root.geometry('{}x{}'.format('230', '150'))
		self.root.resizable(width=False, height=False)

		####### TOP ########
		titulo = Label(self.root, text="DOSSIES MASSIVOS", font=("Arial Black",12))
		titulo.grid(row=0,column=1, pady=15, padx=15)

		###########
		color = "#%02x%02x%02x" % (50, 205, 50)
		self.button = Button(self.root, text="RODAR", bg=color, command=run, width=15)
		self.button.grid(row=4,column=1)

		color_2 = "#%02x%02x%02x" % (237,67,55)
		button2 = Button(self.root, text="Flush DNS", bg=color_2, command=self.flush)
		button2.grid(row=5,column=1)


	def flush(self):
		os.system('ipconfig /flushdns')

##################################################################
def group1():

	banco = pyodbc.connect(connection)
	cursor = banco.cursor()

	pesquisas = ["TJRS" , "TJSP", "TRF2", "TRF3", "TRF5" , "ANOREG_MT"]

	driver = webdriver.Chrome("chromedriver.exe");
	driver.set_page_load_timeout(60)

	for p in pesquisas:

		query = get_query(p)

		remains = True	

		if p == "TJRS":

			while remains:

				for line in cursor.execute(query):

					documento = line[0]
					tipo_documento = line[1]

					if tipo_documento == "FISICA":
						tipo_documento = "Física"
					elif tipo_documento == "JURIDICA":
						tipo_documento = "Jurídica"

					try:
						TJRS.run(documento,tipo_documento)
					except requests.exceptions.Timeout:
						continue

				cursor.execute(query)
				qtd = len(cursor.fetchall())

				if qtd == 0:
					remains = False

		if p == "TJSP":

			while remains:

				for line in cursor.execute(query):

					documento = line[0]
					tipo_documento = line[1]

					if tipo_documento == "FISICA":
						tipo_documento = "Física"
					elif tipo_documento == "JURIDICA":
						tipo_documento = "Jurídica"

					try:
						TJSP.run(documento,tipo_documento,driver)
					except TimeoutException:
						continue

				cursor.execute(query)
				qtd = len(cursor.fetchall())

				if qtd == 0:
					remains = False

		if p == "TRF2":

			while remains:

				for line in cursor.execute(query):

					documento = line[0]
					tipo_documento = line[1]

					if tipo_documento == "FISICA":
						tipo_documento = "Física"
					elif tipo_documento == "JURIDICA":
						tipo_documento = "Jurídica"

					try:
						TRF2.run(documento,tipo_documento)
					except requests.exceptions.Timeout:
						continue

				cursor.execute(query)
				qtd = len(cursor.fetchall())

				if qtd == 0:
					remains = False

		if p == "TRF3":

			while remains:

				for line in cursor.execute(query):

					documento = line[0]
					tipo_documento = line[1]

					if tipo_documento == "FISICA":
						tipo_documento = "Física"
					elif tipo_documento == "JURIDICA":
						tipo_documento = "Jurídica"

					try:
						TRF3.go(documento,tipo_documento,driver)
					except TimeoutException:
						continue

				cursor.execute(query)
				qtd = len(cursor.fetchall())

				if qtd == 0:
					remains = False

		if p == "TRF5":

			while remains:

				for line in cursor.execute(query):

					documento = line[0]
					tipo_documento = line[1]

					if tipo_documento == "FISICA":
						tipo_documento = "Física"
					elif tipo_documento == "JURIDICA":
						tipo_documento = "Jurídica"

					try:
						TRF5.run(documento,tipo_documento)
					except requests.exceptions.Timeout:
						continue

				cursor.execute(query)
				qtd = len(cursor.fetchall())

				if qtd == 0:
					remains = False

		if p == "ANOREG_MT":
			
			while remains:

				ANOREG_MT.log(driver)

				for line in cursor.execute(query):

					documento = line[0]
					tipo_documento = line[1]

					if tipo_documento == "FISICA":
						tipo_documento = "Física"
					elif tipo_documento == "JURIDICA":
						tipo_documento = "Jurídica"

					try:
						ANOREG_MT.run(documento,tipo_documento,driver)
					except TimeoutException:
						continue
					
				cursor.execute(query)
				qtd = len(cursor.fetchall())

				if qtd == 0:
					remains = False

def group2():

	banco = pyodbc.connect(connection)
	cursor = banco.cursor()

	pesquisas = ["BRUSQUE", "CELESC", "ITAJAI",  "SICAR_MT", "SICAR_PA", "SICAR_RO"]

	driver = webdriver.Chrome("chromedriver.exe");
	driver.set_page_load_timeout(60)

	for p in pesquisas:

		query = get_query(p)

		remains = True

		if p == "BRUSQUE":
			
			while remains:

				for line in cursor.execute(query):

					documento = line[0]
					tipo_documento = line[1]

					if tipo_documento == "FISICA":
						tipo_documento = "Física"
					elif tipo_documento == "JURIDICA":
						tipo_documento = "Jurídica"

					try:
						BRUSQUE.go(documento,tipo_documento,driver)
					except TimeoutException:
						continue
					
				cursor.execute(query)
				qtd = len(cursor.fetchall())

				if qtd == 0:
					remains = False

		if p == "CELESC":

			while remains:

				for line in cursor.execute(query):

					documento = line[0]
					tipo_documento = line[1]

					if tipo_documento == "FISICA":
						tipo_documento = "Física"
					elif tipo_documento == "JURIDICA":
						tipo_documento = "Jurídica"

					try:
						CELESC.go(documento,tipo_documento,driver)
					except TimeoutException:
						continue
					
				cursor.execute(query)
				qtd = len(cursor.fetchall())

				if qtd == 0:
					remains = False

		if p == "ITAJAI":

			while remains:

				for line in cursor.execute(query):

					documento = line[0]
					tipo_documento = line[1]

					if tipo_documento == "FISICA":
						tipo_documento = "Física"
					elif tipo_documento == "JURIDICA":
						tipo_documento = "Jurídica"

					try:
						ITAJAI.go(documento,tipo_documento,driver)
					except TimeoutException:
						continue
					
				cursor.execute(query)
				qtd = len(cursor.fetchall())

				if qtd == 0:
					remains = False

		if p == "SICAR_MT":

			while remains:

				for line in cursor.execute(query):

					documento = line[0]
					tipo_documento = line[1]

					if tipo_documento == "FISICA":
						tipo_documento = "Física"
					elif tipo_documento == "JURIDICA":
						tipo_documento = "Jurídica"

					try:
						SICAR_MT.run(documento,tipo_documento)
					except requests.exceptions.Timeout:
						continue
					
				cursor.execute(query)
				qtd = len(cursor.fetchall())

				if qtd == 0:
					remains = False

		if p == "SICAR_PA":

			while remains:

				for line in cursor.execute(query):

					documento = line[0]
					tipo_documento = line[1]

					if tipo_documento == "FISICA":
						tipo_documento = "Física"
					elif tipo_documento == "JURIDICA":
						tipo_documento = "Jurídica"

					try:
						SICAR_PA.run(documento,tipo_documento)
					except requests.exceptions.Timeout:
						continue
					
				cursor.execute(query)
				qtd = len(cursor.fetchall())

				if qtd == 0:
					remains = False

		if p == "SICAR_RO":

			while remains:

				for line in cursor.execute(query):

					documento = line[0]
					tipo_documento = line[1]

					if tipo_documento == "FISICA":
						tipo_documento = "Física"
					elif tipo_documento == "JURIDICA":
						tipo_documento = "Jurídica"

					try:
						SICAR_RO.run(documento,tipo_documento)
					except requests.exceptions.Timeout:
						continue
					
				cursor.execute(query)
				qtd = len(cursor.fetchall())

				if qtd == 0:
					remains = False

def group3():

	banco = pyodbc.connect(connection)
	cursor = banco.cursor()

	pesquisas = ["CELPE",  "COELBA", "SICREDI", "SIGEF", "SPU", "ELEKTRO"]

	driver = webdriver.Chrome("chromedriver.exe");
	driver.set_page_load_timeout(60)

	for p in pesquisas:

		query = get_query(p)

		remains = True

		if p == "CELPE":

			while remains:

				for line in cursor.execute(query):

					documento = line[0]
					tipo_documento = line[1]

					if tipo_documento == "FISICA":
						tipo_documento = "Física"
					elif tipo_documento == "JURIDICA":
						tipo_documento = "Jurídica"
						
					try:
						CELPE.go(documento,tipo_documento,driver)
					except TimeoutException:
						continue
					
				cursor.execute(query)
				qtd = len(cursor.fetchall())

				if qtd == 0:
					remains = False

		if p == "COELBA":

			while remains:

				for line in cursor.execute(query):

					documento = line[0]
					tipo_documento = line[1]

					if tipo_documento == "FISICA":
						tipo_documento = "Física"
					elif tipo_documento == "JURIDICA":
						tipo_documento = "Jurídica"

					try:
						COELBA.go(documento,tipo_documento,driver)
					except TimeoutException:
						continue
					
				cursor.execute(query)
				qtd = len(cursor.fetchall())

				if qtd == 0:
					remains = False

		if p == "SICREDI":

			while remains:

				for line in cursor.execute(query):

					documento = line[0]
					tipo_documento = line[1]

					if tipo_documento == "FISICA":
						tipo_documento = "Física"
					elif tipo_documento == "JURIDICA":
						tipo_documento = "Jurídica"

					try:
						SICREDI.run(documento,tipo_documento,driver)
					except TimeoutException:
						continue
					
				cursor.execute(query)
				qtd = len(cursor.fetchall())

				if qtd == 0:
					remains = False

		if p == "SIGEF":

			while remains:

				for line in cursor.execute(query):

					documento = line[0]
					tipo_documento = line[1]

					if tipo_documento == "FISICA":
						tipo_documento = "Física"
					elif tipo_documento == "JURIDICA":
						tipo_documento = "Jurídica"

					try:
						SIGEF.run(documento,tipo_documento)
					except requests.exceptions.Timeout:
						continue
					
				cursor.execute(query)
				qtd = len(cursor.fetchall())

				if qtd == 0:
					remains = False

		if p == "SPU":

			while remains:

				for line in cursor.execute(query):

					documento = line[0]
					tipo_documento = line[1]

					if tipo_documento == "FISICA":
						tipo_documento = "Física"
					elif tipo_documento == "JURIDICA":
						tipo_documento = "Jurídica"

					try:
						SPU.run(documento,tipo_documento,driver)
					except TimeoutException:
						continue
					
				cursor.execute(query)
				qtd = len(cursor.fetchall())

				if qtd == 0:
					remains = False

		if p == "ELEKTRO":

			while remains:

				for line in cursor.execute(query):

					documento = line[0]
					tipo_documento = line[1]

					if tipo_documento == "FISICA":
						tipo_documento = "Física"
					elif tipo_documento == "JURIDICA":
						tipo_documento = "Jurídica"

					try:
						ELEKTRO.run(documento,tipo_documento)
					except requests.exceptions.Timeout:
						continue
					
				cursor.execute(query)
				qtd = len(cursor.fetchall())

				if qtd == 0:
					remains = False

def group4():

	banco = pyodbc.connect(connection)
	cursor = banco.cursor()

	pesquisas = [ "CENPROT", "CNDT", "COELBA", "BETHA"]

	driver = webdriver.Chrome("chromedriver.exe");
	driver.set_page_load_timeout(60)

	for p in pesquisas:

		query = get_query(p)

		remains = True

		if p == "CENPROT":

			while remains:

				for line in cursor.execute(query):

					documento = line[0]
					tipo_documento = line[1]

					if tipo_documento == "FISICA":
						tipo_documento = "Física"
					elif tipo_documento == "JURIDICA":
						tipo_documento = "Jurídica"

					try:
						CENPROT.go(documento,tipo_documento,driver)
					except TimeoutException:
						continue
					
				cursor.execute(query)
				qtd = len(cursor.fetchall())

				if qtd == 0:
					remains = False

		if p == "CNDT":

			while remains:

				for line in cursor.execute(query):

					documento = line[0]
					tipo_documento = line[1]

					if tipo_documento == "FISICA":
						tipo_documento = "Física"
					elif tipo_documento == "JURIDICA":
						tipo_documento = "Jurídica"

					try:
						CNDT.go(documento,tipo_documento,driver)
					except TimeoutException:
						continue
					
				cursor.execute(query)
				qtd = len(cursor.fetchall())

				if qtd == 0:
					remains = False

		if p == "COELBA":

			while remains:

				for line in cursor.execute(query):

					documento = line[0]
					tipo_documento = line[1]

					if tipo_documento == "FISICA":
						tipo_documento = "Física"
					elif tipo_documento == "JURIDICA":
						tipo_documento = "Jurídica"

					try:
						COELBA.go(documento,tipo_documento,driver)
					except TimeoutException:
						continue
					
				cursor.execute(query)
				qtd = len(cursor.fetchall())

				if qtd == 0:
					remains = False

		if p == "BETHA":

			driver.quit()

			betha = BETHA_M.Betha(cedente)
			betha.run_massivo()

def get_query(parametro):

	if parametro == "BRUSQUE":

		query = """DECLARE @CIDADE AS VARCHAR(50) = 'BRUSQUE' SELECT DOCUMENTO_PESSOA,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}') 
		AND NOT EXISTS (SELECT DOCUMENTO_PESSOA FROM DB_JIVE_2017.DBO.IMOVEIS WHERE DBO.DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = DBO.IMOVEIS.DOCUMENTO_PESSOA AND CIDADE = @CIDADE)""".format(cedente)
	
		return query

	if parametro == "ITAJAI":

		query = """DECLARE @CIDADE AS VARCHAR(50) = 'ITAJAI' SELECT DOCUMENTO_PESSOA,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}') 
		AND NOT EXISTS (SELECT DOCUMENTO_PESSOA FROM DB_JIVE_2017.DBO.IMOVEIS WHERE DBO.DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = DBO.IMOVEIS.DOCUMENTO_PESSOA AND CIDADE = @CIDADE)""".format(cedente)

		return query

	if parametro == "COELBA":

		query = """SELECT DOCUMENTO_PESSOA ,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}')
		AND NOT EXISTS (SELECT DOCUMENTO_PESSOA FROM DB_JIVE_2017.DBO.CENTRAIS_ENERGETICAS WHERE DB_JIVE_2017.DBO.DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = DB_JIVE_2017.DBO.CENTRAIS_ENERGETICAS.DOCUMENTO_PESSOA 
		AND FONTE_PESQUISA = 'COELBA')""".format(cedente)

		return query

	if parametro == "CELPE":

		query = """SELECT DOCUMENTO_PESSOA ,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}')
		AND NOT EXISTS (SELECT DOCUMENTO_PESSOA FROM DB_JIVE_2017.DBO.CENTRAIS_ENERGETICAS WHERE DB_JIVE_2017.DBO.DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = DB_JIVE_2017.DBO.CENTRAIS_ENERGETICAS.DOCUMENTO_PESSOA 
		AND FONTE_PESQUISA = 'CELPE')""".format(cedente)

		return query

	if parametro == "CELESC":

		query = """SELECT DOCUMENTO_PESSOA ,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}')
		AND NOT EXISTS (SELECT DOCUMENTO_PESSOA FROM DB_JIVE_2017.DBO.CENTRAIS_ENERGETICAS WHERE DB_JIVE_2017.DBO.DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = DB_JIVE_2017.DBO.CENTRAIS_ENERGETICAS.DOCUMENTO_PESSOA 
		AND FONTE_PESQUISA = 'CELESC')""".format(cedente)

		return query

	if parametro == "ELEKTRO":

		query = """SELECT DOCUMENTO_PESSOA ,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}')
		AND NOT EXISTS (SELECT DOCUMENTO_PESSOA FROM DB_JIVE_2017.DBO.CENTRAIS_ENERGETICAS WHERE DB_JIVE_2017.DBO.DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = DB_JIVE_2017.DBO.CENTRAIS_ENERGETICAS.DOCUMENTO_PESSOA 
		AND FONTE_PESQUISA = 'ELEKTRO')""".format(cedente)
		return query

	if parametro == "CNDT":

		query = """SELECT DOCUMENTO_PESSOA,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}')
		AND NOT EXISTS (SELECT DOCUMENTO_PESSOA FROM DB_JIVE_2017.DBO.CNDT WHERE DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = DB_JIVE_2017.dbo.CNDT.DOCUMENTO_PESSOA)""".format(cedente)

		return query

	if parametro == "CENPROT":
		query = """SELECT DOCUMENTO_PESSOA,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}')
		AND NOT EXISTS (SELECT DOCUMENTO_PESSOA FROM DB_JIVE_2017.dbo.CENPROT WHERE DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = DB_JIVE_2017.dbo.CENPROT.DOCUMENTO_PESSOA)""".format(cedente)
		
		return query

	if parametro == "SICAR_PA":

		query = """SELECT DOCUMENTO_PESSOA,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}') 
		AND NOT EXISTS (SELECT DOCUMENTO_PESSOA FROM DB_JIVE_2017.DBO.IMOVEIS_CAR WHERE DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = IMOVEIS_CAR.DOCUMENTO_PROPRIETARIO and UF = 'PA')""".format(cedente)
		
		return query

	if parametro == "SICAR_RO":

		query = """SELECT DOCUMENTO_PESSOA,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}') 
		AND NOT EXISTS (SELECT DOCUMENTO_PESSOA FROM DB_JIVE_2017.DBO.IMOVEIS_CAR WHERE DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = IMOVEIS_CAR.DOCUMENTO_PROPRIETARIO and UF = 'RO')""".format(cedente)

		return query

	if parametro == "SICAR_MT":

		query = """SELECT DOCUMENTO_PESSOA,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}') 
		AND NOT EXISTS (SELECT DOCUMENTO_PESSOA FROM DB_JIVE_2017.DBO.IMOVEIS_CAR WHERE DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = IMOVEIS_CAR.DOCUMENTO_PROPRIETARIO and UF = 'MT')""".format(cedente)

		return query 

	if parametro == "SICREDI":

		query = """SELECT DOCUMENTO_PESSOA,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}') 
		AND NOT EXISTS (SELECT DOCUMENTO_PESSOA FROM DB_JIVE_2017.DBO.COOPERATIVA_SICREDI WHERE DBO.DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = DBO.COOPERATIVA_SICREDI.DOCUMENTO_PESSOA)""".format(cedente)
			
		return query

	if parametro == "SIGEF":

		query = """SELECT DOCUMENTO_PESSOA,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}') 
		AND NOT EXISTS (SELECT DOCUMENTO_PESSOA FROM DB_JIVE_2017.DBO.SIGEF WHERE DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = SIGEF.DOCUMENTO_PROPRIETARIO)""".format(cedente)
		 
		return query

	if parametro == "SPU":

		query = """SELECT DOCUMENTO_PESSOA,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}') 
		AND NOT EXISTS (SELECT DOCUMENTO_PROPRIETARIO FROM DB_JIVE_2017.DBO.IMOVEIS_SPU WHERE DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = IMOVEIS_SPU.DOCUMENTO_PROPRIETARIO)""".format(cedente)
		
		return query

	if parametro == "TRF2":

		query = """SELECT DOCUMENTO_PESSOA,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}') 
		AND NOT EXISTS (SELECT DOCUMENTO_PESSOA FROM DB_JIVE_2017.DBO.PRECATORIOS WHERE DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = PRECATORIOS.DOCUMENTO_PESSOA and FONTE_PESQUISA = 'TRF2')""".format(cedente)
		
		return query

	if parametro == "TRF5":	

		query = """SELECT DOCUMENTO_PESSOA,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}') 
		AND NOT EXISTS (SELECT DOCUMENTO_PESSOA FROM DB_JIVE_2017.DBO.PRECATORIOS WHERE DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = PRECATORIOS.DOCUMENTO_PESSOA and FONTE_PESQUISA = 'TRF5')""".format(cedente)
		
		return query

	if parametro == "TRF3":

		query = """SELECT DOCUMENTO_PESSOA,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}') 
		AND NOT EXISTS (SELECT DOCUMENTO_PESSOA FROM DB_JIVE_2017.DBO.PRECATORIOS WHERE DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = PRECATORIOS.DOCUMENTO_PESSOA and FONTE_PESQUISA = 'TRF3')""".format(cedente)
		
		return query

	if parametro == "TJRJ":

		query = """SELECT DOCUMENTO_PESSOA,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}') 
		AND NOT EXISTS (SELECT DOCUMENTO_PESSOA FROM DB_JIVE_2017.DBO.PRECATORIOS WHERE DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = PRECATORIOS.DOCUMENTO_PESSOA and FONTE_PESQUISA = 'TJRJ')""".format(cedente)

		return query

	if parametro == "TJRS":

		query = """SELECT DOCUMENTO_PESSOA,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}') 
		AND NOT EXISTS (SELECT DOCUMENTO_PESSOA FROM DB_JIVE_2017.DBO.PRECATORIOS WHERE DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = PRECATORIOS.DOCUMENTO_PESSOA and FONTE_PESQUISA = 'TJRS')""".format(cedente)

		return query

	if parametro == "TJSP":

		query = """SELECT DOCUMENTO_PESSOA,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}') 
		AND NOT EXISTS (SELECT DOCUMENTO_PESSOA FROM DB_JIVE_2017.DBO.PRECATORIOS WHERE DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = PRECATORIOS.DOCUMENTO_PESSOA and FONTE_PESQUISA = 'TJSP')""".format(cedente)
			
		return query

	if parametro == "ANOREG_MT":

		query = """SELECT DOCUMENTO_PESSOA,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}')
		AND NOT EXISTS (SELECT DOCUMENTO_PESSOA FROM DB_JIVE_2017.DBO.CERTIDOES_ANOREG WHERE DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = CERTIDOES_ANOREG.DOCUMENTO_PESSOA)""".format(cedente)

		return query

	if parametro == "SIGEF":

		query = """SELECT DOCUMENTO_PESSOA,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}') 
		AND NOT EXISTS (SELECT DOCUMENTO_PROPRIETARIO FROM DB_JIVE_2017.DBO.SIGEF WHERE DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = SIGEF.DOCUMENTO_PROPRIETARIO)""".format(cedente)

		return query
		
	if parametro == "MIAMIDADE":

		query = """SELECT DOCUMENTO_PESSOA,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}') 
		AND NOT EXISTS (SELECT DOCUMENTO_PESSOA FROM DB_JIVE_2017.DBO.MIAMIDADE WHERE DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = MIAMIDADE.DOCUMENTO_PESSOA)""".format(cedente)

		return query
		
	if parametro == "SUNBIZ":

		query = """SELECT DOCUMENTO_PESSOA,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}') 
		AND NOT EXISTS (SELECT DOCUMENTO_PESSOA FROM DB_JIVE_2017.DBO.SUNBIZ WHERE DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = SUNBIZ.DOCUMENTO_PESSOA)""".format(cedente)

		return query

	if parametro == "OPENCORPORATES":

		query = """SELECT DOCUMENTO_PESSOA,TIPO_PESSOA FROM DB_JIVE_2017.dbo.DEVEDORES_PRECIFICACAO WHERE CEDENTE IN ('{0}') 
		AND NOT EXISTS (SELECT DOCUMENTO_PESSOA FROM DB_JIVE_2017.DBO.OPENCORPORATES WHERE DEVEDORES_PRECIFICACAO.DOCUMENTO_PESSOA = OPENCORPORATES.DOCUMENTO_PESSOA)""".format(cedente)

		return query

	print ("Não existe query : %s" %parametro)	
	return None

	########################	
		
def run():

	logger.info("<Start: Execução Iniciada>")

	threads = []

	thread = Thread(target=group1, args=())
	threads.append(thread)

	thread = Thread(target=group2, args=())
	threads.append(thread)

	thread = Thread(target=group3, args=())
	threads.append(thread)

	thread = Thread(target=group4, args=())
	threads.append(thread)

	for thread in threads:
		thread.start()

	for thread in threads:
		thread.join()

	print("FINALIZADO")

if __name__ == "__main__":

	connection = '.'

	logger = logging.getLogger("DOSSIE_MASSIVO")
	logger.setLevel(logging.DEBUG)
	fh = logging.FileHandler(r"C:\files\prd\logs\log.log")
	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	fh.setFormatter(formatter)
	logger.addHandler(fh)

	cedente = "SANY"

	menu = Menu()
	menu.root.mainloop()

