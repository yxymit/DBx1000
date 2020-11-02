apt update
# apt install linux-aws
apt install -y git
git clone https://github.com/yuxiamit/dbx1000_logging.git
cd dbx1000_logging
git checkout array-lock-free
apt install -y libboost-dev build-essential numactl python python3-scipy python3-pip
python3 -m pip install matplotlib
mkfs.ext4 -E nodiscard /dev/xvdb
mkfs.ext4 -E nodiscard /dev/xvdc
mkfs.ext4 -E nodiscard /dev/xvdd
mkfs.ext4 -E nodiscard /dev/xvde
mkdir -p /data0
mkdir -p /data1
mkdir -p /data2
mkdir -p /data3
sudo mount -o rw /dev/xvdb /data0
sudo mount -o rw /dev/xvdc /data1
sudo mount -o rw /dev/xvdd /data2
sudo mount -o rw /dev/xvde /data3
python tools/compile.py
sudo python3 scripts/runExpr.py shortEC2
