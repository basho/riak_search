#!/usr/bin/env sh

# compare.sh compares the contents of STDIN with the contents of the
# provided filename. 
#
# It assumes that both STDIN and the file are:
#
#   - in XML format
#   - are the output of /solr/select
#   - include the "id" field
#
# To process, it:
#   1) greps the file and STDIN for the Regex /name="id"/
#   2) uses sed to extract the actual id value
#   3) sorts the id's
#   4) checks for any unique id's meaning that a result is found
#      in the file but not the query or vice-versa.

# Check for verbose mode...
VERBOSE=0
if [ "$1" == "-v" ] || [ "$1" == "-verbose" ]; then
    VERBOSE=1
    shift
fi

# Check for a file parameter...
if [ $# -ne 1 ]; then
    echo "Usage: $0 [-v] <FILE>"
    echo "Expects results from a /solr/select on STDIN, in XML format."
    exit
fi

# Pull the ids from standard input...
PARSEDINPUT=`tr "{" "\n" | grep -e "\"id\"\s*:" | sed -e 's/.*\"id\"\s*:\"\([^\"]*\)\".*/\1/' | sort | uniq`

# Pull the ids from the provided file...
PARSEDFILE=`cat $1 | tr "{" "\n" | grep -e "\"id\"\s*:" | sed -e 's/.*\"id\"\s*:\"\([^\"]*\)\".*/\1/' | sort | uniq`

# Compare the files for differing lines...
COUNT=`echo "$PARSEDFILE\n$PARSEDINPUT" | sort | uniq -u | wc -l`

# If we're in verbose mode, print out the differences. It's too
# complicated to attributed the differences to the file where they
# occurred, so instead, just print a list of ids.
if [ $COUNT -ge 1 ] && [ $VERBOSE -eq 1 ]; then
    DIFFS=`echo "$PARSEDFILE\n$PARSEDINPUT" | sort | uniq -u`
    echo $COUNT difference\(s\):
    echo "--"
    echo "$DIFFS"   
    echo "--"
fi

# Exit status is 0 on success, or the number of lines that were
# different on failure.
exit $COUNT
