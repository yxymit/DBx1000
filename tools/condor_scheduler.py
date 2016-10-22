import os

class CondorScheduler:
	def __init__(self):
		self.jobqueue = []
		self.environment = "GetEnv = True\n" +\
			"Universe = vanilla\n" +\
			"Notification = Error\n" +\
			"should_transfer_files = IF_NEEDED\n" +\
			"WhenToTransferOutput = ON_EXIT\n" +\
			"Executable = /bin/bash\n" +\
			"Log = /tmp/echo.$ENV(USER).log\n" +\
			"RequestCpus = 80\n" +\
			"RequestMemory = 20480\n" +\
			"Requirements = isSwarm\n" +\
			"Environment = LD_LIBRARY_PATH=/usr/lib/\n" +\
			"Rank = isSwarm\n\n" 

	def addJob(self, command, output_dir):
		self.jobqueue.append(output_dir)
		if not os.path.exists(output_dir): 
			os.makedirs(output_dir)
		command_file = open("%s/condor.sh" % output_dir, 'w')
		command_file.write(command)
		command_file.close()


	def generateSubmitFile(self):
		submit_file = open("tools/condor.submit", 'w')
		submit_file.write(self.environment)
		for output_dir in self.jobqueue:
			submit_file.write("Arguments = %s/condor.sh\n" % output_dir)
			submit_file.write("Error = %s/output\n" % output_dir)
			submit_file.write("Output = %s/output\n" % output_dir)
			submit_file.write("Queue\n\n")
		submit_file.close()
