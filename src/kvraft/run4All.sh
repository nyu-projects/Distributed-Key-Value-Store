#!/bin/bash

counter=1
while [ $counter -lt 100 ]
do
    echo $counter
    go test -v >> test4
    ERROR="$(grep FAIL test4 | wc -l)"
    echo $ERROR
    if [ "$ERROR" -ne 0 ]; then
        break
    fi  
    rm test4
    ((counter++))
done
