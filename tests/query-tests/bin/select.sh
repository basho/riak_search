#!/usr/bin/env sh

# select.sh runs a solr query against /solr/select and displays
# the results on stdout.

BASE_DIR=`dirname $0`

# Check for the correct number of parameters...
if [ $# -ne 2 ]; then
    echo "Usage: $0 <http://localhost:8983/solr> <QUERY>"
    exit
fi

# URLEncode the query...
QUERY=`echo "$2" | sed -f $BASE_DIR/urlencode.sed`

# Run the query...
URL="$1/select?indent=on&version=2.2&q=$QUERY&start=0&rows=10000&fl=id&qt=standard&wt=standard"
echo $URL
curl --silent $URL