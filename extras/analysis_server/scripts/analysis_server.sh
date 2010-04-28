#!/bin/bash

PORT=6098

java -server -Xms16m -Xmx64m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -cp rel/lib/lucene-core-3.0.1.jar:rel/lib/netty-3.1.5.GA.jar:rel/lib/protobuf-java-2.3.0.jar:rel/lib/analysis_master-0.1.jar com.basho.search.analysis.server.AnalysisServer $PORT
