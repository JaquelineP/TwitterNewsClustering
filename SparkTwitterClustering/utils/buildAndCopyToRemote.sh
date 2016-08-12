#!/bin/bash
cd ..
#mvn clean package

if [[ -z "$1" || -z "$2" ]]; then
	echo "usage: ./buildAndCopyToRemote.sh [path_to_key] [remote_host]"

else
	scp -i $1 target/SparkTwitterClustering-jar-with-dependencies.jar $2:~
	scp -i $1 src/main/resources/twitter.dat $2:~
	scp -i $1 utils/driver_bootstrap.sh $2:~
	scp -i $1 utils/runOnCluster.sh $2:~
fi

