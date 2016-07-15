#!/bin/bash

rm runtime.csv

echo "Start deploying on server"

# for loop from node 1 to 20
for ((node = 12; node>=1; node=node-2))
do

        # run each setting 3 times
        for j in `seq 1 1`;
        do
                echo "Run with $node nodes "
                echo "$node," >> runtime.csv
                spark-submit --class de.hpi.isg.mmds.sparkstreaming.Main \
                --driver-memory 4g --executor-memory 2g --total-executor-cores $node --num-executors $node \
                target/SparkTwitterClustering-jar-with-dependencies.jar \
                -input hdfs:///twitter.dat \
                -tweetsPerBatch 10000 -maxBatchCount 10 -runtime  >> runtime.csv
                echo "   " >> runtime.csv
        done
done

# tear down
# echo "remove file from hdfs"
#/opt/hadoop/default/bin/hadoop fs -rm /data/mmds16/twitter/twitter.dat

echo "Done. See runtime.csv for results."
