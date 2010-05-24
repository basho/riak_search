#!/bin/bash

java -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:CMSInitiatingOccupancyFraction=70 -d64 -server -Djava.library.path=/usr/local/lib:java.library.path=/usr/local/lib:/usr/local/BerkeleyDB.4.8/lib -Xms$1m -Xmx$2m -Dfile.encoding=UTF-8 -cp db.jar:log4j-1.2.15.jar:lucene-core-3.0.0.jar:netty-3.1.5.GA.jar:netty-protobuf-rpc-1.0.0:protobuf-java-2.3.1-pre.jar:raptor-0.1.jar raptor.server.RaptorServer $3
