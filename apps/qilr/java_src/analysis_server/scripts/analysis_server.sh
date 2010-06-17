#!/bin/bash

SCRIPT_LIBS=$1/lib

java -server -Xms16m -Xmx64m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -cp lucene-core-3.0.1.jar:netty-3.1.5.GA.jar:protobuf-java-2.3.0.jar:analysis_master-0.1.jar:analysis_extensions.jar com.basho.search.analysis.server.AnalysisServer $1
