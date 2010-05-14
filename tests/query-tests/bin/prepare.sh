#!/usr/bin/env sh

# prepare.sh runs a solr query against /solr/select and writes
# the results to the provided file.

BASE_DIR=`dirname $0`

# Check for the correct number of parameters...
if [ $# -ne 3 ]; then
    echo "Usage: $0 <http://localhost:8983/solr> <QUERY> <FILENAME>"
    exit
fi

# URLEncode the query...
QUERY=`echo "$2" | sed -f $BASE_DIR/urlencode.sed`

# Run the query...
URL="$1/select?indent=on&version=2.2&q=$QUERY&start=0&rows=10000&fl=id&qt=standard&wt=json"
curl --silent $URL > $3