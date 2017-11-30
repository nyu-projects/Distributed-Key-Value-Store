#!/bin/bash

counter=1
while [ $counter -lt 100 ]
do
    echo $counter
    go test -run TestBasic >> test4A
    go test -run TestConcurrent >> test4A
    go test -run TestUnreliable >> test4A
    go test -run TestUnreliableOneKey  >> test4A
    go test -run TestOnePartition >> test4A
    go test -run TestManyPartitionsOneClient >> test4A
    go test -run TestManyPartitionsManyClients >> test4A
    go test -run TestPersistOneClient >> test4A
    go test -run TestPersistConcurrent >> test4A
    go test -run TestPersistConcurrentUnreliable >> test4A
    go test -run TestPersistPartition >> test4A
    go test -run TestPersistPartitionUnreliable >> test4A

    ERROR="$(grep FAIL test4A | wc -l)"
	echo KV
    echo $ERROR
    if [ "$ERROR" -ne 0 ]; then
        break
    fi
    rm test4A

    go test -run TestSnapshotRPC >> test4B
    go test -run TestSnapshotSize >> test4B
    go test -run TestSnapshotRecover >> test4B
    go test -run TestSnapshotRecoverManyClients >> test4B
    go test -run TestSnapshotUnreliable >> test4B
    go test -run TestSnapshotUnreliableRecover >> test4B
    go test -run TestSnapshotUnreliableRecoverConcurrentPartition >> test4B

    ERROR="$(grep FAIL test4B | wc -l)"
	echo SNAPSHOT
    echo $ERROR
    if [ "$ERROR" -ne 0 ]; then
        break
    fi  
    rm test4B

    ((counter++))
done
