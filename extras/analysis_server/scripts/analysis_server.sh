#!/bin/bash

PORT=6098

java -server -Xms16m -Xmx64m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -cp lib/lucene-core-3.0.1.jar:lib/netty-3.1.5.GA.jar:lib/protobuf-java-2.3.0.jar:lib/analysis_master-0.1.jar com.basho.search.analysis.server.AnalysisServer $PORT
