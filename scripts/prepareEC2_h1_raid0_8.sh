echo 'preparing md0'
umount /data0
umount /data1
umount /data2
umount /data3
umount /data4
umount /data5
umount /data6
umount /data7
# the parameters recommended by EC2 do not work, only 1x performance
# trying 64kb chunk size
umount /dev/md0
mdadm --stop /dev/md0
mdadm --remove /dev/md0
# sleep 5
# mdadm --create /dev/md0 --metadata=0.90 --level=0 --chunk=32 --raid-devices=8 /dev/sd[ab]1
mdadm --create --verbose /dev/md0 --level=0 --raid-devices=8 /dev/xvd[b-i]
# mdadm --create --verbose /dev/md0 --chunk=128 --metadata=0.90 --level=0 --raid-devices=8 /dev/xvd[b-i]
# mkfs.ext3 -b 4096 -E stride=16,stripe-width=128 -F /dev/md0 # https://uclibc.org/~aldot/mkfs_stride.html # this config results in 438M/s
# smaller chunk size is better for large I/Os according to 
mkfs.ext4 -F /dev/md0
# mkfs.xfs -f /dev/md0 # recommended by stackoverflow
mkdir -p /data0
mount /dev/md0 /data0
