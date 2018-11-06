import logging
logger = logging.getLogger("Listener.modules.captcha")

def solve_captcha(image):
	from antigate import AntiGate , AntiGateError

	sent = False
	while not sent:
		try:
			gate = AntiGate('.')            
			captcha_id = gate.send(image)
			answer = (gate.get(captcha_id))
			return (answer)

		except AntiGateError as error:
			import _email
			logger.error('<ERROR: %s>'%error)
			_email.send('CAPTCHA',error," ")
			break

def solve_recaptcha(url,webkey):

	from antigate import AntiGate
	import json , requests , time
 
	clientkey = "."

	send = {
	"clientKey":clientkey,
	"task":
		{
			"type":"NoCaptchaTaskProxyless",
			"websiteURL":url,
			"websiteKey":webkey
		},
	"softId":0,
	"languagePool":"pt-BR"
	}

	r = requests.post('https://api.anti-captcha.com/createTask ', json=send)
	data = json.loads(r.text)
	taskId = (data['taskId'])
	
	get = {
	"clientKey":clientkey,
	"taskId": taskId
	}

	done = False
	while not done:
		time.sleep(10)
		r = requests.post('https://api.anti-captcha.com/getTaskResult', json=get)
		data = json.loads(r.text)

		if data['status'] == 'processing':
			continue
		elif data['status'] == 'ready':
			captcha_response = (data['solution']['gRecaptchaResponse'])
			done = True

	return (captcha_response)


