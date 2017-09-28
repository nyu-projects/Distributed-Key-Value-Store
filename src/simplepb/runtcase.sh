#!/bin/bash

counter=1
while [ $counter -lt 100 ]
do
    echo $counter
    go test -v >> tfile
    ((counter++))
done

grep FAIL tfile | wc -l
grep PASS tfile | wc -l
