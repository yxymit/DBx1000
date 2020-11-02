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
	disksContent = subprocess.check_output("lsblk | tail -n %d" % n, shell=True).decode('utf-8')
	disks = disksContent.split('\n')
	dlist = []

	for i in range(n):
		check_output_ignore_error("sudo umount /data%d" % i, shell=True)

	check_output_ignore_error("sudo umount /dev/md0", shell=True)
	check_output_ignore_error("sudo mdadm --stop /dev/md0", shell=True)
	check_output_ignore_error("sudo mdadm --remove /dev/md0", shell=True)


	for d in disks:
		if d:
			print(d)
			dname = d.split()[0]
			dlist.append('/dev/' + dname)

	assert len(dlist) == n

	subprocess.check_output("yes | sudo mdadm --create --verbose /dev/md0 --level=0 --raid-devices=8 " + " ".join(dlist), shell=True)

	subprocess.check_output("sudo mkfs.ext4 -F /dev/md0", shell=True)
	subprocess.check_output("sudo mkdir -p /data0", shell=True)
	subprocess.check_output("sudo mount /dev/md0 /data0", shell=True)

if __name__ == "__main__":
	if len(sys.argv) > 1:
		n = int(sys.argv[1])
	run()
