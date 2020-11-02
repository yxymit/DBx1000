import subprocess
import sys
n = 8

def check_output_ignore_error(cmd, shell=True):
	try:
		output = subprocess.check_output(cmd, shell=shell)
	except Exception as e:
		output = e.output
	return output

def run():

	for i in range(n):
		check_output_ignore_error("sudo umount /data%d" % i, shell=True)

	check_output_ignore_error("sudo umount /dev/md0", shell=True)
	check_output_ignore_error("sudo mdadm --stop /dev/md0", shell=True)
	check_output_ignore_error("sudo mdadm --remove /dev/md0", shell=True)

	# check_output_ignore_error("sudo mount -t tmpfs -o size=200G,huge=always,mpol=interleave tmpfs /data0", shell=True)
	# check_output_ignore_error("sudo mount -t ramfs ramfs /data0", shell=True)

	check_output_ignore_error("sudo mount -t tmpfs -o size=20G,huge=always,mpol=bind:0 tmpfs0 /data0", shell=True)
	check_output_ignore_error("sudo mount -t tmpfs -o size=20G,huge=always,mpol=bind:1 tmpfs1 /data1", shell=True)

if __name__ == "__main__":
	
	if len(sys.argv) > 1:
		n = int(sys.argv[1])
		
	run()