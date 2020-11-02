# all i3en.metal figures

# v1.2-254-gf12546db main baselines
# v1.2-282-g8aab0b8e turn off locktable for serial baselines for fair comparison
# v1.2-285-g711f9e3c use TPC-C 80 warehouses, contain new TPC-C results and throughput-fixed LD, BD, and VD for YCSB
# till v1.2-285-g711f9e3c_0 is the data we submitted to revision.

# for camera ready, we get the DRAM SILO_R performance in results10-03-2020-21-33-58/results/short16-YCSB-ramv1.2-286-gc26667f4/

# PREFIX=~/efs/results
# PREFIX=./ec2/res/results08-13-2020-20-29-05/results
PREFIX=./ec2/res/results08-15-2020-19-39-23/results

# CameraReady

# PREFIX2=./ec2/res/results10-03-2020-21-56-21/results
# PREFIX2=./ec2/res/results10-04-2020-15-24-18/results
PREFIX2=./ec2/res/results10-04-2020-20-19-13/results

# main results
python3 scripts/generateFigures.py shortEC2_16 ${PREFIX}/short16-fixbs-v1.2-254-gf12546db ${PREFIX}/short16-fixbs-v1.2-282-g8aab0b8e ${PREFIX}/short16-fixbs-v1.2-285-g711f9e3c

python3 scripts/generateFigures.py shortEC2_16Silo ${PREFIX}/short16-fixbs-silov1.2-254-gf12546db ${PREFIX}/short16-fixbs-silov1.2-282-g8aab0b8e ${PREFIX}/short16-fixbs-silov1.2-285-g711f9e3c

# DRAM

python3 scripts/generateFigures.py shortEC2_16RAMDISK ${PREFIX}/short16-YCSB-ramv1.2-254-gf12546db/ ${PREFIX}/short16-YCSB-ramv1.2-282-g8aab0b8e/ ${PREFIX2}/short16-YCSB-ramv1.2-286-gc26667f4/

# Transaction Impact

# python3 scripts/generateFigures.py sweepR16 ${PREFIX}/sensR16-ycsb500-v1.2-282-g8aab0b8e/
python3 scripts/generateFigures.py sweepR16 ${PREFIX}/sensR16-ycsb500-v1.2-254-gf12546db

# python3 scripts/generateFigures.py timeBreakDown ${PREFIX}/sensR16-ycsb500-v1.2-282-g8aab0b8e/
# we have set STAT_VERBOSE to 0 at version 282, therefore non-useable.
python3 scripts/generateFigures.py timeBreakDown ${PREFIX}/sensR16-ycsb500-v1.2-254-gf12546db

python3 scripts/generateFigures.py simd ${PREFIX}/simd-ycsb10g-v1.2-281-ga24b1f82

# TPCF

python3 scripts/generateFigures.py shortEC2FULLTPCC_8 ${PREFIX}/short-fulltpcc-v1.2-226-g3c9f1517/

python3 scripts/generateFigures.py tpccBreakDown ${PREFIX}/short-fulltpcc-v1.2-226-g3c9f1517/

# HDD results

python3 scripts/generateFigures.py all8 ${PREFIX}/short-hddv1.2-254-gf12546db ${PREFIX}/short-hddv1.2-282-g8aab0b8e/ ${PREFIX}/short-hddv1.2-285-g711f9e3c/

python3 scripts/generateFigures.py sensitivity8 ${PREFIX}/sensitivityv1.2-254-gf12546db ${PREFIX}/sensitivityv1.2-282-g8aab0b8e/

