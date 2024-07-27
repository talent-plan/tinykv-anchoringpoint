#!/bin/bash

# settings to change
times=10
project="3b"
removelog=1

# don't change
if [ ! -d "./test_output" ]; then
    mkdir "./test_output"
fi

logdir="./test_output/${project}"
if [ ! -d $logdir ]; then
    mkdir $logdir
fi
lastdir="${logdir}/`date +%Y%m%d%H%M%S`"
if [ ! -d $lastdir ]; then
    mkdir $lastdir
fi
summary="${lastdir}/summary.log"
echo "times pass fail panic runtime" >> $summary

totalpass=0
totalfail=0
totalpanic=0
totalruntime=0

for i in $(seq 1 $times)
do
    logfile="${lastdir}/$i.log"
    start=$(date +%s)
    echo "make project${project} $i times"
    make project${project} >> $logfile
    end=$(date +%s)
    pass_count=$(grep -i "PASS" $logfile | wc -l)
    echo "pass count: $pass_count"
    fail_count=$(grep "FAIL" $logfile | wc -l)
    echo "fail count: $fail_count"
    panic_count=$(grep -i "panic" $logfile | wc -l)
    echo "panic count: $panic_count"
    runtime=$((end-start))
    echo "$i $pass_count $fail_count $panic_count $runtime" >> $summary

    totalpass=$((totalpass+pass_count))
    totalfail=$((totalfail+fail_count))
    totalpanic=$((totalpanic+panic_count))
    totalruntime=$((totalruntime+runtime))

    # if pass, remove the log
    if [ $removelog -eq 1 ]; then
        if [ $panic_count -lt 0 ]; then
            rm $logfile
        fi
        sleep 5
    fi
done

echo "total $totalfail $totalpanic $totalruntime" >> $summary