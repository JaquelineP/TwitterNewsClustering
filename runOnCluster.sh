#!/bin/bash

cd SparkTwitterClustering
rm runtime.csv

echo "Start deploying on server"

# for loop from node 1 to 20
for node in `seq 1 20`;
do
	# run each setting 3 times
	for j in `seq 1 3`;
	do
		echo "Run with $node nodes "
		echo "$node," >> runtime.csv
		/opt/spark/default/bin/spark-submit --class de.hpi.isg.mmds.sparkstreaming.StreamingKMeansExample \
		--driver-memory 4g --executor-memory 2g --total-executor-cores $node \
		target/SparkTwitterClustering-jar-with-dependencies.jar \
		-input hdfs://tenemhead2/data/mmds16/twitter/twitter.dat \
		-tweetsPerBatch 10 -batchDuration 1 -maxBatchCount 20 -runtime  >> runtime.csv
		echo "   " >> runtime.csv
	done
done

# tear down
echo "remove file from hdfs"
/opt/hadoop/default/bin/hadoop fs -rm /data/mmds16/twitter/twitter.dat

echo "Done. See runtime.csv for results."
