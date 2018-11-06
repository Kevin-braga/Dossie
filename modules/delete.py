import logging
logger = logging.getLogger("Listener.modules.delete")

def remove(caminho,parametro):

	output = []
	try:
		f = open (caminho, "r")
		arquivo = f.read()
		for line in arquivo.split(';'):
			if not parametro in line:
				output.append(line+';')
		f.close()

		f = open(caminho, 'w')
		f.writelines(output)			
		f.close()

	except FileNotFoundError as error:
		logger.error('<ERROR: %s>'%error)
