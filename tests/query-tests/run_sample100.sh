#!/usr/bin/env sh

### BEGIN INITIALIZE VARS ###
if [ $# -ne 1 ]; then
    echo "Usage: $0 <http://localhost:8983/solr>"
    exit -1
fi

SOLR=$1; BASE=`dirname $0`
UPDATE="$BASE/bin/update.sh $SOLR"
SELECT="$BASE/bin/select.sh $SOLR"
COMPARE="$BASE/bin/compare.sh"
. $BASE/bin/shtest.sh

function run_test {
    $SELECT "$1" | $COMPARE $2
    test "$1"; `$SELECT "$1" | $COMPARE $2`; end
}

### RUN TESTS ###
start_tests "Sample100 Unit Tests"
# $UPDATE sample_data/sample100.add

# ### RUN SIMPLE TESTS
# start_tests "Simple Queries"
# run_test "acc:abc" results/100/simple_1.xml
# run_test "color:red" results/100/simple_2.xml
# run_test "parity:even" results/100/simple_3.xml
# run_test "color:(red blue)" results/100/simple_4.xml
# end_tests

# ### RUN 'AND' TESTS
# start_tests "'AND' Operator"
# run_test "acc:afa AND color:red" results/100/and_1.xml
# run_test "acc:afa AND color:red AND parity:even" results/100/and_2.xml
# end_tests

# ### RUN 'OR' TESTS
# start_tests "'OR' Operator"
# run_test "color:(red OR blue)" results/100/or_1.xml
# run_test "color:red OR color:blue" results/100/or_2.xml
# run_test "color:red OR parity:odd" results/100/or_3.xml # FILED BUG
# run_test "color:red OR parity:odd OR key:sample100_aab" results/100/or_4.xml # FILED BUG
# end_tests

# ### RUN 'NOT' TESTS
# start_tests "'NOT' Operator"
# run_test "acc:(aab AND NOT aac)" results/100/not_1.xml
# run_test "acc:(aab AND NOT aba)" results/100/not_2.xml
# run_test "acc:(aab AND (NOT aac))" results/100/not_1.xml
# run_test "acc:(aab AND (NOT aba))" results/100/not_2.xml
# end_tests

# ### RUN +/- TESTS
# start_tests "+/- Operators"
# run_test "-acc:AAD" results/100/plusminus_1.xml # TIMES OUT
# run_test "+acc:aab AND -acc:aac" results/100/plusminus_2.xml
# run_test "+acc:aab AND -acc:aeb" results/100/plusminus_3.xml
# run_test "acc:(aab AND -aac)" results/100/plusminus_2.xml
# run_test "acc:(aab AND -aeb)" results/100/plusminus_3.xml
# run_test "-acc:AEB -parity:even -color:red -color:orange -color:yellow" results/100/plusminus_4.xml
# end_tests

# ### RUN GROUPING TESTS
# start_tests "Grouping"
# run_test "(color:red OR color:blue) AND (acc:aja)" results/100/grouping_1.xml
# run_test "(color:red AND parity:even) OR (color:blue AND parity:odd)" results/100/grouping_2.xml
# run_test "(color:red AND (parity:even OR key:sample100_abe)) OR ((color:blue OR key:sample100_abc) AND parity:odd)" results/100/grouping_3.xml
# end_tests

# ### RUN RANGE TESTS
# start_tests "Ranges"
# # run_test "key:{sample100_aaa TO sample100_aaj}" results/100/range_1.xml
# run_test "color:{aaa TO ccc}" results/100/range_2.xml
# run_test "color:{blue TO yellow}" results/100/range_3.xml
# end_tests

# ### RUN PREFIX TESTS
# start_tests "Prefixes and Wildcards"
# run_test "color:re*" results/100/wildcard_1.xml
# run_test "color:red*" results/100/wildcard_2.xml
# run_test "color:re?" results/100/wildcard_3.xml
# end_tests

# ### RUN FUZZY TESTS
# start_tests "Fuzzy Matching"
# run_test "color:rad~" results/100/fuzzy_1.xml
# run_test "color:blum~" results/100/fuzzy_2.xml
# # end_tests

### RUN COMPLEX TESTS
start_tests "Complex Queries"
# run_test "(color:re* OR color:blub~) AND (parity:{d TO f})" results/100/complex_1.xml
# run_test "(acc:afa AND -acc:aga) AND -color:oran*" results/100/complex_2.xml
run_test "(acc:afa AND (NOT acc:aga)) AND (NOT color:oran*)" results/100/complex_2.xml
# run_test "acc:(afa NOT aga) AND -color:oran*" results/100/complex_3.xml
run_test "acc:(afa AND (NOT aga)) AND (NOT color:oran*)" results/100/complex_3.xml
end_tests

# $UPDATE sample_data/sample100.delete
end_tests

