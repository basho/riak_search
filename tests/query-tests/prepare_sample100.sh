#!/usr/bin/env sh

### BEGIN INITIALIZE VARS ###
if [ $# -ne 1 ]; then
    echo "Usage: $0 <http://localhost:8983/solr>"
    exit -1
fi

SOLR=$1; BASE=`dirname $0`
UPDATE="$BASE/bin/update.sh $SOLR"
PREPARE="$BASE/bin/prepare.sh $SOLR"
mkdir -p results/100

function prepare {
    echo "Preparing Test: $1"
    $PREPARE "$1" "$2"
}

### INSERT DATA
$UPDATE sample_data/sample100.add

### PREPARE SIMPLE TESTS
prepare "acc_t:ABC" results/100/simple_1.xml
prepare "color_t:red" results/100/simple_2.xml
prepare "parity_t:even" results/100/simple_3.xml
prepare "color_t:(red blue)" results/100/simple_4.xml

### PREPARE 'AND' TESTS
prepare "acc_t:AFA AND color_t:red" results/100/and_1.xml
prepare "acc_t:AFA AND color_t:red AND parity_t:even" results/100/and_2.xml

### PREPARE 'OR' TESTS
prepare "color_t:(red OR blue)" results/100/or_1.xml
prepare "color_t:red OR color_t:blue" results/100/or_2.xml
prepare "color_t:red OR parity_t:odd" results/100/or_3.xml
prepare "color_t:red OR parity_t:odd OR key_t:sample100_AAB" results/100/or_4.xml

### PREPARE 'NOT' TESTS
prepare "acc_t:(AAB NOT AAC)" results/100/not_1.xml
prepare "acc_t:(AAB NOT ABA)" results/100/not_2.xml

### PREPARE +/- TESTS
prepare "-acc_t:AAD" results/100/plusminus_1.xml
prepare "+acc_t:AAB -acc_t:AAC" results/100/plusminus_2.xml
prepare "+acc_t:AAB -acc_t:AEB" results/100/plusminus_3.xml
prepare "-acc_t:AEB -parity_t:even -color_t:red -color_t:orange -color_t:yellow" results/100/plusminus_4.xml

### PREPARE GROUPING TESTS
prepare "(color_t:red OR color_t:blue) AND (acc_t:AJA)" results/100/grouping_1.xml
prepare "(color_t:red AND parity_t:even) OR (color_t:blue AND parity_t:odd)" results/100/grouping_2.xml
prepare "(color_t:red AND (parity_t:even OR key_t:sample100_ABE)) OR ((color_t:blue OR key_t:sample100_ABC) AND parity_t:odd)" results/100/grouping_3.xml

### PREPARE RANGE TESTS
prepare "key_t:{sample100_AAA TO sample100_AAJ}" results/100/range_1.xml
prepare "color_t:{a TO c}" results/100/range_2.xml
prepare "color_t:{blue TO yellow}" results/100/range_3.xml

### PREPARE PREFIX TESTS
prepare "color_t:r*" results/100/wildcard_1.xml
prepare "color_t:red*" results/100/wildcard_2.xml
prepare "color_t:r??" results/100/wildcard_3.xml

### PREPARE FUZZY TESTS
prepare "color_t:rad~" results/100/fuzzy_1.xml
prepare "color_t:blum~" results/100/fuzzy_2.xml

### PREPARE COMPLEX TESTS
prepare "(color_t:re* OR color_t:blub~) AND (parity_t:{d TO f})" results/100/complex_1.xml
prepare "(acc_t:AFA AND -acc_t:AGA) AND -color_t:oran*" results/100/complex_2.xml
prepare "acc_t:(AFA NOT AGA) AND -color_t:oran*" results/100/complex_3.xml

### REMOVE DATA
$UPDATE sample_data/sample100.delete
