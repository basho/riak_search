#!/usr/bin/env sh

# update.sh writes or updates a solr interface with documents using
# by calling /solr/update. 

if [ $# -ne 2 ]; then
    echo "Usage: $0 <http://localhost:8983/solr> <FILE>"
    exit
fi

URL="$1/update"

# Upload...
RESULTS=`curl -X POST -H text/xml --data-binary @$2 $URL`
if [ $? -ne 0 ]; then
    echo "$RESULTS"
    echo "Error running 'curl $URL'"
    exit -1
fi
