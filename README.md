# DBx1000_logging
The logging module of the DBx1000 database.

We include a number of logging algorithms in this module:

+ [Taurus](https://arxiv.org/abs/2010.06760)
+ Silo-R
+ Serial
+ Plover

The full TPC-C implementation is in the *fulltpcc* branch.

Thank you for your interest in this logging module. Please feel free to submit issues. Contributions are welcome.

### Dependencies and Compiling

To install dependencies from Ubuntu:

	sudo sh scripts/prepareEC2_metal.sh

To compile the baselines:

	python tools/compile

### Reproduce the Experiments

We also include a script (ec2/runspot.py) to automate the EC2 setup. Please fill in the configuration file (ec2/config.ini) before you run the setup script.

To reproduce the experiments:

	python scripts/runExpr.py run_all

Specifically, the experiments on *h1_16xlarge* are included in

	python scripts/runExpr.py allhdd

And the experiments on *i3_en.metal* are included in

	python scripts/runExpr.py all16

The plotting script is at *scripts/generateFigures.py*.
