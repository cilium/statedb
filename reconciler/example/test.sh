#!/usr/bin/env bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR

exec 3>&1
exec &> test.log

set -eux

go build .
./example &

PID=$!
cleanup() {
  s=$?
  set +eux
  kill $PID
  if [ "$s" -ne 0 ]; then
    cat test.log >&3
    echo "FAILED" >&3
  else
    echo "PASS" >&3
  fi
  kill -9 $PID
  exit $s
}
trap 'cleanup' SIGINT SIGTERM EXIT

for i in 1 2 3 4 5; do
  curl -q http://localhost:8080/statedb &>/dev/null && break
  sleep 0.5
done

: Insertion
curl -d 'hello world' http://localhost:8080/memos/test
for i in 1 2 3 4 5; do
  test -f memos/test && break
  sleep 0.5
done
test -f memos/test

: Full reconciliation
rm -f memos/test
for i in 1 2 3 4 5; do
  test -f memos/test && break
  sleep 1
done
test -f memos/test 

: Deletion
curl -XDELETE http://localhost:8080/memos/test
for i in 1 2 3 4 5; do
  test ! -f memos/test && break
  sleep 0.5
done
test ! -f memos/test

