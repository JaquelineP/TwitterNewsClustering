#!/bin/bash

rm runtime.csv

echo "Start deploying on server"

batchSizes=( 10000 20000 30000)

for batchSize in "${batchSizes[@]}"
do
	for ((node = 12; node>=1; node=node-2))
	do

		# run each setting 3 times
		for j in `seq 1 3`;
		do
			echo "Run with $node nodes and batch size $batchSize"
			echo "$node," >> runtime.csv
			spark-submit --class de.hpi.isg.mmds.sparkstreaming.Main \
			--total-executor-cores $node --num-executors $node \
			SparkTwitterClustering-jar-with-dependencies.jar \
			-input hdfs:///twitter.dat \
			-tweetsPerBatch $batchSize -maxBatchCount 10 -runtime  >> runtime.csv
			echo "   " >> runtime.csv
		done
	done
done
# tear down
# echo "remove file from hdfs"
#/opt/hadoop/default/bin/hadoop fs -rm /data/mmds16/twitter/twitter.dat

echo "Done. See runtime.csv for results."
