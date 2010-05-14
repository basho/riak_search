# shtest.sh - Bash Unit Tests

function start_tests { 
    if [ "$ST_LVL" == "" ] || [ $ST_LVL -eq 0 ]; then
	echo "---"
	echo "Begin Unit Tests: $0"
	echo "---"
	ST_LVL=0
    fi
    ST_LVL=$(( $ST_LVL + 1 ))
    ST_PASS[$ST_LVL]=0
    ST_FAIL[$ST_LVL]=0
    padding
    echo
    echo "${ST_PAD}Test: $1" 
}

function test  { 
    ST_TEST_NAME[$ST_LVL]=$1
}

function end {
    if [ $? -eq 0 ]; then
	padding
	echo "$ST_PAD[.] ${ST_TEST_NAME[$ST_LVL]}"
	for (( i=1; i<=$ST_LVL; i++ )); do
	    ST_PASS[$i]=$(( ${ST_PASS[$i]} + 1 ))
	done
    else
	padding 
	echo "$ST_PAD[X] ${ST_TEST_NAME[$ST_LVL]}"
	for (( i=1; i<=$ST_LVL; i++ )); do
	    ST_FAIL[$i]=$(( ${ST_FAIL[$i]} + 1 ))
	done
    fi   
}

function end_tests {
    TOTAL=$(( ${ST_PASS[$ST_LVL]} + ${ST_FAIL[$ST_LVL]} ))
    if [ ${ST_FAIL[$ST_LVL]} -eq 0 ]; then
	padding
	echo "$ST_PAD[PASSED] Passed all ${ST_PASS[$ST_LVL]} test(s)."
	echo
	ST_LVL=$(( $ST_LVL - 1 ))
	if [ $ST_LVL -eq 0 ]; then exit 0; fi
    else
	padding
	echo "$ST_PAD[FAILED] Failed ${ST_FAIL[$ST_LVL]} of $TOTAL tests!"
	echo
	ST_LVL=$(( $ST_LVL - 1 ))
	if [ $ST_LVL -eq 0 ]; then exit $ST_FAIL; fi
    fi
}

function padding {
    ST_PAD=""
    for (( i=1; i<=$ST_LVL; i++ )); do
	ST_PAD="$ST_PAD  "
    done
}