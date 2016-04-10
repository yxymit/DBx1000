#!/bin/sh

echo "./rundb -Lb10" >> res.txt
./rundb -Lb10 > output.txt
python optimization/optimize.py >> res.txt

echo "./rundb -Lb20" >> res.txt
./rundb -Lb20 > output.txt
python optimization/optimize.py >> res.txt

echo "./rundb -Lb30" >> res.txt
./rundb -Lb30 > output.txt
python optimization/optimize.py >> res.txt

echo "./rundb -Lb40" >> res.txt
./rundb -Lb40 > output.txt
python optimization/optimize.py >> res.txt

echo "./rundb -Lb50" >> res.txt
./rundb -Lb50 > output.txt
python optimization/optimize.py >> res.txt

echo "./rundb -Lb60" >> res.txt
./rundb -Lb60 > output.txt
python optimization/optimize.py >> res.txt

echo "./rundb -Lb70" >> res.txt
./rundb -Lb70 > output.txt
python optimization/optimize.py >> res.txt

echo "./rundb -Lb80" >> res.txt
./rundb -Lb80 > output.txt
python optimization/optimize.py >> res.txt

echo "./rundb -Lb90" >> res.txt
./rundb -Lb90 > output.txt
python optimization/optimize.py >> res.txt

