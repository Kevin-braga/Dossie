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
############################################

class MyHandler(PatternMatchingEventHandler):

	patterns = ["*.txt"]

	def on_created(self,event):

		logger.info(event)

		documento = str(os.path.basename(event.src_path).replace(".txt",""))

		menu.add_to_list(documento)

		if os.path.exists(event.src_path):

			shutil.move(event.src_path, dir_[1])

			f = open (dir_[1]+"\%s.txt"%documento, 'r')
			parametros = f.read()
			f.close()

			thread = Thread(target=self.call_dossie, args=(documento,parametros))	

			thread.start()

	def call_dossie(self,documento,parametros):

		new_dossie = Dossie(documento,parametros)
		new_dossie.pesquisa()
		new_dossie.finalizar()

##################################################################		

class Menu(object):
	def __init__(self):

		self.root = Tk()
		self.root.title("Pesquisas")
		self.root.geometry('{}x{}'.format('230', '480'))
		self.root.resizable(width=False, height=False)

		####### TOP ########
		titulo = Label(self.root, text="MODULO DOSSIE", font=("Arial Black",12))
		titulo.grid(row=0,column=1, pady=15, padx=15)

		_help = Button(self.root, text = "?", command=self._help_, width=3, bg="gold")
		_help.grid(row=0,column=2)

		titulo = Label(self.root, text="Fila")
		titulo.grid(row=1,column=1)

		self.listbox = Listbox(self.root,height=18)
		self.listbox.grid(row=2,column=1)

		####### BOT ########
		sub_titulo = Label(self.root, text="STATUS", font=("Arial",8))
		sub_titulo.grid(row=3,column=1)

		###########
		color = "#%02x%02x%02x" % (50, 205, 50)
		self.button = Button(self.root, text="AGUARDANDO", bg=color, state=DISABLED, width=15)
		self.button.grid(row=4,column=1)

		color_2 = "#%02x%02x%02x" % (237,67,55)
		button2 = Button(self.root, text="Flush DNS", bg=color_2, command=self.flush)
		button2.grid(row=5,column=1)


	def flush(self):
		os.system('ipconfig /flushdns')

	def remove_from_list(self,value):
		self.listbox.delete(value)

	def add_to_list(self,value):
		self.listbox.insert(END, value)

	def update_status(self,status):

		if status == 1:
			color = '#%02x%02x%02x' % (64, 204, 208)
			self.button.config(text='RODANDO',bg=color)
			self.root.update()

		if status == 2:
			color = "#%02x%02x%02x" % (50, 205, 50)
			self.button.config(text='AGUARDANDO',bg=color)
			self.root.update()


	def _help_(self):
		tkinter.messagebox.showinfo('Help', """Desenvolvido por Kevin Braga""")

##################################################################

class Dossie():

	logger = logging.getLogger("Menu.dossie")

	def __init__(self,d,p):

		self.documento = d
		self.parametros = p.split(';')

	def pesquisa(self):

		logger.info("<Start: Execução Iniciada - [%s] - Consultas %s >"%(self.documento,self.parametros))

		#	TIPO DE PESSOA   #
		if "FISICA" in self.parametros:
			self.tipo_documento = "Física"

		elif "JURIDICA" in self.parametros:
			self.tipo_documento = "Jurídica"

		########################	
		#	BETHA   #
		betha = Thread(target=BETHA.Betha, args=(self.documento,self.tipo_documento))
		betha.start()
		########################
		# 	NEOWAY  # 
		if self.tipo_documento == "Física":
			neoway = NEOWAY.fisica(self.documento)
		elif self.tipo_documento == "Jurídica":
			neoway = NEOWAY.juridica(self.documento)

		self.nome = neoway.consulta()

		print(self.nome)
		##############

		self.driver = webdriver.Chrome("chromedriver.exe");
		self.driver.set_page_load_timeout(60)

		for word in self.parametros:

			if  word == "SIGEF":
				try:
					SIGEF.run(self.documento,self.tipo_documento)
					logger.info("SIGEF finalizado. [%s]"%self.documento)
					delete.remove(dir_[1]+"\%s.txt"%self.documento,"SIGEF")

				except requests.exceptions.Timeout:
					logger.warning("<Warning: SIGEF Tempo Limite excedido - [%s]>"%self.documento)
					pass

			if  word == "CENPROT":
				try:
					CENPROT.go(self.documento,self.tipo_documento,self.driver)
					logger.info("CENPROT finalizado. [%s]"%self.documento)
					delete.remove(dir_[1]+"\%s.txt"%self.documento,"CENPROT")

				except TimeoutException:
					logger.warning("<Warning: CENPROT Tempo Limite excedido - [%s]>"%self.documento)
					pass

			if  word == "PRECATORIO":
				try:
					TJRS.run(self.documento,self.tipo_documento)
					logger.info("TJRS finalizado. [%s]"%self.documento)

				except requests.exceptions.Timeout:
					logger.warning("<Warning: TJRS Tempo Limite excedido - [%s]>"%self.documento)
					pass

				try:
					TJSP.run(self.documento,self.tipo_documento,self.driver)
					logger.info("TJSP finalizado. [%s]"%self.documento)

				except TimeoutException:
					logger.warning("<Warning: TJSP Tempo Limite excedido - [%s]>"%self.documento)
					pass

				try:
					TRF2.run(self.documento,self.tipo_documento)
					logger.info("TRF2 finalizado. [%s]"%self.documento)

				except requests.exceptions.Timeout:
					logger.warning("<Warning: TRF2 Tempo Limite excedido - [%s]>"%self.documento)
					pass

				try:
					TRF3.go(self.documento,self.tipo_documento,self.driver)
					logger.info("TRF3 finalizado. [%s]"%self.documento)

				except TimeoutException:
					logger.warning("<Warning: TRF3 Tempo Limite excedido - [%s]>"%self.documento)
					pass

				try:
					TRF5.run(self.documento, self.tipo_documento)
					logger.info("TRF5 finalizado. [%s]"%self.documento)
					delete.remove(dir_[1]+"\%s.txt"%self.documento,"PRECATORIOS")

				except requests.exceptions.Timeout:
					logger.warning("<Warning: TRF5 Tempo Limite excedido - [%s]>"%self.documento)
					pass


			if word == "SPU":
				try:
					SPU.run(self.documento,self.tipo_documento,self.driver)
					logger.info("SPU finalizado. [%s]"%self.documento)
					delete.remove(dir_[1]+"\%s.txt"%self.documento,"SPU")

				except TimeoutException:
					logger.warning("<Warning: SPU Tempo Limite excedido - [%s]>"%self.documento)
					pass

			if  word == "ANOREG":
				try:
					ANOREG_MT.go(self.documento,self.tipo_documento,self.driver)
					###
					logger.info("ANOREG_MT finalizado. [%s]"%self.documento)
					###
					delete.remove(dir_[1]+"\%s.txt"%self.documento,"ANOREG")

				except TimeoutException:
					logger.warning("<Warning: ANOREG_MT Tempo Limite excedido - [%s]>"%self.documento)
					pass

			if  word == "SICREDI":
				try:
					SICREDI.run(self.documento,self.tipo_documento,self.driver)
					###	
					logger.info("SICREDI finalizado. [%s]"%self.documento)
					###
					delete.remove(dir_[1]+"\%s.txt"%self.documento,"SICREDI")

				except TimeoutException:
					logger.warning("<Warning: SICREDI Tempo Limite excedido - [%s]>"%self.documento)
					pass
				
			if  word == "CNDT":
				try:
					CNDT.go(self.documento,self.tipo_documento,self.driver)
					###
					logger.info("CNDT finalizado. [%s]"%self.documento)
					###
					delete.remove(dir_[1]+"\%s.txt"%self.documento,"CNDT")

				except TimeoutException:
					logger.warning("<Warning: CNDT Tempo Limite excedido - [%s]>"%self.documento)
					pass

			if  word == "SICAR":
				try:
					SICAR_PA.run(self.documento,self.tipo_documento)
					###
					logger.info("SICAR_PA finalizado. [%s]"%self.documento)

				except requests.exceptions.Timeout:
					logger.warning("<Warning: SICAR_PA Tempo Limite excedido - [%s]>"%self.documento)
					pass

				try:
					SICAR_RO.run(self.documento,self.tipo_documento)
					###
					logger.info("SICAR_RO finalizado. [%s]"%self.documento)
					###
					delete.remove(dir_[1]+"\%s.txt"%self.documento,"SICAR")

				except requests.exceptions.Timeout:
					logger.warning("<Warning: SICAR_RO Tempo Limite excedido - [%s]>"%self.documento)
					pass

				try:
					SICAR_MT.run(self.documento,self.tipo_documento)
					###
					logger.info("SICAR_MT finalizado. [%s]"%self.documento)
					###
					delete.remove(dir_[1]+"\%s.txt"%self.documento,"SICAR")

				except requests.exceptions.Timeout:
					logger.warning("<Warning: SICAR_MT Tempo Limite excedido - [%s]>"%self.documento)
					pass

			if  word == "CENTRAIS":
				try:
					ELEKTRO.run(self.documento, self.tipo_documento)
					###
					logger.info("ELEKTRO finalizado. [%s]"%self.documento)

				except requests.exceptions.Timeout:
					logger.warning("<Warning: ELEKTRO Tempo Limite excedido - [%s]>"%self.documento)
					pass

				try:
					CELESC.go(self.documento,self.tipo_documento,self.driver)
					###
					logger.info("CELESC finalizado. [%s]"%self.documento)

				except TimeoutException:
					logger.warning("<Warning: CELESC Tempo Limite excedido - [%s]>"%self.documento)
					pass	

				try:
					CELPE.go(self.documento,self.tipo_documento,self.driver)
					###
					logger.info("CELPE finalizado. [%s]"%self.documento)

				except TimeoutException:
					logger.warning("<Warning: CELPE Tempo Limite excedido - [%s]>"%self.documento)
					pass

				try:
					COELBA.go(self.documento,self.tipo_documento,self.driver)
					###
					logger.info("COELBA finalizado. [%s]"%self.documento)
					###
					delete.remove(dir_[1]+"\%s.txt"%self.documento,"CENTRAIS")

				except TimeoutException:
					logger.warning("<Warning: COELBA Tempo Limite excedido - [%s]>"%self.documento)
					pass

			if  word == "IPTU":
				try:
					BRUSQUE.go(self.documento,self.tipo_documento,self.driver)
					###
					logger.info("BRUSQUE finalizado. [%s]"%self.documento)

				except TimeoutException:
					logger.warning("<Warning: BRUSQUE Tempo Limite excedido - [%s]>"%self.documento)
					pass
				try:
					ITAJAI.go(self.documento,self.tipo_documento,self.driver)
					###
					logger.info("ITAJAI finalizado. [%s]"%self.documento)

					delete.remove(dir_[1]+"\%s.txt"%self.documento,"IPTU")

				except TimeoutException:
					logger.warning("<Warning: ITAJAI Tempo Limite excedido - [%s]>"%self.documento)
					pass

			if word == "INTERNACIONAIS":
				try:
					opencorps = OPENCORPORATES.Opencorporates(self.documento,self.nome)
					opencorps.run()
					###
					logger.info("OPENCORPORATES Finalizado. [%s]"%self.documento)

				except requests.exceptions.Timeout:
					logger.warning("<Warning: OPENCORPORATES Tempo Limite excedido - [%s]>"%self.documento)
					pass

				try:
					sunbiz = SUNBIZ.Sunbiz(self.documento,self.nome)
					sunbiz.run()
					###
					logger.info("SUNBIZ Finalizado. [%s]"%self.documento)

				except requests.exceptions.Timeout:
					logger.warning("<Warning: SUNBIZ Tempo Limite excedido - [%s]>"%self.documento)
					pass

				try:
					miami = MIAMIDADE.Miamidade(self.documento,self.nome)
					miami.run()
					###
					logger.info("MIAMIDADE Finalizado. [%s]"%self.documento)
					###
					delete.remove(dir_[1]+"\%s.txt"%self.documento,"INTERNACIONAIS")

				except requests.exceptions.Timeout:
					logger.warning("<Warning: MIAMIDADE Tempo Limite excedido - [%s]>"%self.documento)
					pass


		self.driver.quit()

		betha.join()

		print('Kakaka')

		logger.info("BETHA finalizado. [%s]"%self.documento)


	def finalizar(self):

		menu.remove_from_list([i for i,x in enumerate(menu.listbox.get(0,END)) if x == self.documento])

		if os.path.exists(dir_[1]+"\%s.txt"%self.documento):

			update.atualizar(self.documento,self.tipo_documento)

			os.remove(dir_[1]+"\%s.txt"%self.documento)

			logger.info("<End: Fim da execução - [%s]>"%self.documento)

		else:
			logger.warning("<Warning: Arquivo '%s.txt' não está existe>"%self.documento)


if __name__ == "__main__":

	dir_ = [
	r"C:\files\prd",
	r"C:\files\prd\processando"]

	logger = logging.getLogger("Menu")
	logger.setLevel(logging.DEBUG)
	fh = logging.FileHandler(r"C:\files\prd\logs\log.log")
	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	fh.setFormatter(formatter)
	logger.addHandler(fh)


	event_handler = MyHandler()
	observer = Observer()
	observer.schedule(event_handler, path=dir_[0], recursive=False)
	observer.start()
	
	menu = Menu()
	menu.root.mainloop()

