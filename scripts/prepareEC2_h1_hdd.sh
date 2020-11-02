sudo umount /dev/md0
umount /data0
umount /data1
umount /data2
umount /data3
umount /data4
umount /data5
umount /data6
umount /data7
sudo mdadm --stop /dev/md0
sudo mdadm --remove /dev/md0
yes | mkfs.ext4 -E nodiscard /dev/xvdb
yes | mkfs.ext4 -E nodiscard /dev/xvdc
yes | mkfs.ext4 -E nodiscard /dev/xvdd
yes | mkfs.ext4 -E nodiscard /dev/xvde
mkdir -p /data0
mkdir -p /data1
mkdir -p /data2
mkdir -p /data3
sudo mount -o rw /dev/xvdb /data0
sudo mount -o rw /dev/xvdc /data1
sudo mount -o rw /dev/xvdd /data2
sudo mount -o rw /dev/xvde /data3