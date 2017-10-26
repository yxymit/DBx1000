import os

class BasicScheduler:
	def __init__(self):
		self.jobqueue = []

	def addJob(self, command, output_dir):
		self.jobqueue.append(output_dir)
		if not os.path.exists(output_dir): 
			os.makedirs(output_dir)
		command_file = open("%s/basic.sh" % output_dir, 'w')
		command_file.write(command)
		command_file.close()

	def generateSubmitFile(self):
		submit_file = open("tools/start.sh", 'w')
		for output_dir in self.jobqueue:
			#if not os.path.exists('./%s/output' % output_dir):
			submit_file.write("echo %s\n" % output_dir)
			submit_file.write(". ./%s/basic.sh\n" % output_dir)
		#submit_file.write('python tools/send_email.py')	
		submit_file.close()
