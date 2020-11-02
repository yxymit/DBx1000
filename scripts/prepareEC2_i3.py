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

	ret = check_output_ignore_error('lsblk').decode()
	all = True
	for k in range(n):
		if not ('/data%d' % k) in ret:
			all = False
	if all: # already i3
		print('Already NVMe setup.')
		return

	for i in range(n):
		check_output_ignore_error("sudo umount /data%d" % i, shell=True)

	check_output_ignore_error("sudo umount /dev/md0", shell=True)
	check_output_ignore_error("sudo mdadm --stop /dev/md0", shell=True)
	check_output_ignore_error("sudo mdadm --remove /dev/md0", shell=True)

	disksContent = subprocess.check_output("lsblk | tail -n %d" % n, shell=True).decode('utf-8')
	disks = disksContent.split('\n')
	dlist = []

	for d in disks:
		if d:
			print(d)
			dname = d.split()[0]
			dlist.append(dname)
			print(subprocess.check_output("sudo mkfs.ext4 -E nodiscard /dev/%s" % dname, shell=True).decode('utf-8'))

	for i in range(n):
		subprocess.check_output("sudo mkdir -p /data%d" % i, shell=True)
		subprocess.check_output("sudo mount -o rw,sync /dev/%s /data%d" % (dlist[i], i), shell=True)

if __name__ == "__main__":
	if len(sys.argv) > 1:
		n = int(sys.argv[1])
	run()
