#!/bin/bash
mvn clean package

if [ -z "$1" ]; then
	echo "usage: ./buildAndCopyToRemote.sh [remote_host]"

else
	scp -i ~/.ssh/Amazon-HPI.pem target/SparkTwitterClustering-jar-with-dependencies.jar $1:~
	scp -i ~/.ssh/Amazon-HPI.pem src/main/resources/twitter.dat $1:~
	scp -i ~/.ssh/Amazon-HPI.pem bootstrap.sh $1:~
fi

