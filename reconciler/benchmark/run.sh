#!/usr/bin/env bash
set -eu
go build .

for batchSize in 100 1000 10000; do
  for incrBatchSize in 1000 5000; do
    echo "batchSize: $batchSize, incrBatchSize: $incrBatchSize"
    go run . -objects=100000 -batchsize=$batchSize -incrbatchsize=$incrBatchSize
    echo "----------------------------------------------------"
  done
done
