sudo umount /dev/md0
sudo mdadm --stop /dev/md0
sudo mdadm --remove /dev/md0
yes | mkfs.ext4 -E nodiscard /dev/xvdb
yes | mkfs.ext4 -E nodiscard /dev/xvdc
yes | mkfs.ext4 -E nodiscard /dev/xvdd
yes | mkfs.ext4 -E nodiscard /dev/xvde
yes | mkfs.ext4 -E nodiscard /dev/xvdf
yes | mkfs.ext4 -E nodiscard /dev/xvdg
yes | mkfs.ext4 -E nodiscard /dev/xvdh
yes | mkfs.ext4 -E nodiscard /dev/xvdi
mkdir -p /data0
mkdir -p /data1
mkdir -p /data2
mkdir -p /data3
mkdir -p /data4
mkdir -p /data5
mkdir -p /data6
mkdir -p /data7
sudo mount -o rw /dev/xvdb /data0
sudo mount -o rw /dev/xvdc /data1
sudo mount -o rw /dev/xvdd /data2
sudo mount -o rw /dev/xvde /data3
sudo mount -o rw /dev/xvdf /data4
sudo mount -o rw /dev/xvdg /data5
sudo mount -o rw /dev/xvdh /data6
sudo mount -o rw /dev/xvdi /data7
