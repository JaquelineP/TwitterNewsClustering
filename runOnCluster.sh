#!/bin/bash

# Script should be startet inside TwitterTextMining folder
cd ..

echo "upload data to hdfs"
/opt/hadoop/default/bin/hadoop fs -put twitter.dat /data/mmds16/twitter/twitter.dat

cd TwitterTextMining/SparkTwitterClustering
echo "get new code"
git pull
echo "build jar"
mvn package

echo "Start deploying on server"

# for loop from node 1 to 20
until [ $node -le 19 ]
do
	node=`expr $node + 1`
	cores=`expr $node * 2`

	# run each setting 3 times
	until [ $a -le 2]
	do
		echo "Run with $node nodes and $cores cores"
		echo "$node,$cores," >> runtime.csv
		/opt/spark/default/bin/spark-submit --class de.hpi.isg.mmds.sparkstreaming.StreamingKMeansExample \
		--driver-memory 4g --executor-memory 2g --executor-cores $cores --num-executors $node \
		target/SparkTwitterClustering-jar-with-dependencies.jar \
		-input hdfs://tenemhead2/data/mmds16/twitter/twitter.dat \
		-tweetsPerBatch 5000 -batchDuration 1 -maxBatchCount 10 -runtime true >> runtime.csv
		echo "\n" >> runtime.csv
	done
done

# tear down
echo "remove file from hdfs"
/opt/hadoop/default/bin/hadoop fs -rm /data/mmds16/twitter/twitter.dat

echo "Done. See runtime.csv for results."