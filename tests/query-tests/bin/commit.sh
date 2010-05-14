#!/usr/bin/env sh

# commit.sh commits a RiakDB update.

if [ $# -ne 1 ]; then
    echo "Usage: $0 <http://localhost:8983/solr>"
    exit
fi

URL="$1/update"

# Now commit...
RESULTS=`echo "<commit />" | curl $URL -v -H "Content-Type: text/xml" -d @-`
if [ $? -ne 0 ]; then
    echo "$RESULTS"
fi

