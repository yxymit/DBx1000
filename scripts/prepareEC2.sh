apt update
# apt install linux-aws
apt install -y libboost-dev
apt install -y build-essential
apt install -y numactl
apt install -y zsh
chsh -s /usr/bin/zsh
mkfs.ext4 -E nodiscard /dev/nvme0n1
mkfs.ext4 -E nodiscard /dev/nvme2n1
mkfs.ext4 -E nodiscard /dev/nvme3n1
mkfs.ext4 -E nodiscard /dev/nvme4n1
mkdir -p /data0
mkdir -p /data1
mkdir -p /data2
mkdir -p /data3
sudo mount -o rw /dev/nvme0n1 /data0
sudo mount -o rw /dev/nvme2n1 /data1
sudo mount -o rw /dev/nvme3n1 /data2
sudo mount -o rw /dev/nvme4n1 /data3
python tools/compile.py
sudo python3 scripts/runExpr.py shortEC2
