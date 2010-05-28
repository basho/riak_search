#!/bin/bash

java -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:CMSInitiatingOccupancyFraction=70 -d64 -server -Djava.library.path=/usr/local/lib:java.library.path=/usr/local/lib:/usr/local/BerkeleyDB.4.8/lib -Xms$1m -Xmx$2m -Dfile.encoding=UTF-8 -cp .:./db.jar:./google-collect-1.0.jar:./log4j-1.2.15.jar:./lucene-core-3.0.0.jar:./netty-3.1.5.GA.jar:./protobuf-java-2.3.1-pre.jar:./raptor.jar  raptor.server.RaptorServer $3 $4
