#!/bin/bash

JARS=".:./google-collect-1.0.jar:./log4j-1.2.15.jar"
JARS="$JARS:./lucene-core-3.0.0.jar:./netty-3.1.5.GA.jar"
JARS="$JARS:./protobuf-java-2.3.1-pre.jar:./clhm-release-1.0-lru.jar:./raptor.jar"
JARS="$JARS:./je-4.0.103.jar"

java -XX:+UseConcMarkSweepGC -XX:+UseParNewGC \
     -XX:CMSInitiatingOccupancyFraction=70 \
     -d64 -server -Xms$1m -Xmx$2m \
     -Dfile.encoding=UTF-8 \
     -cp $JARS \
     raptor.server.RaptorServer $3 $4
