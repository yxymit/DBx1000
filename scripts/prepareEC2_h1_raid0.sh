echo 'preparing md0'
umount /data0
umount /data1
umount /data2
umount /data3
mdadm --create --verbose /dev/md0 --chunk=128 --metadata=0.9 --level=0 --raid-devices=4 /dev/xvd[b-e]
mkfs.ext4 -F /dev/md0
mkdir -p /data0
mount /dev/md0 /data0
