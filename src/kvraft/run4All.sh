#!/bin/bash

counter=1
while [ $counter -lt 100 ]
do
    echo $counter
    go test -v >> test4All
    ERROR="$(grep FAIL test4All | wc -l)"
    echo $ERROR
    if [ "$ERROR" -ne 0 ]; then
        break
    fi  
    rm test4All
    ((counter++))
done
