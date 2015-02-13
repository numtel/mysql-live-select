#!/bin/bash
# Append test suite results to oot.log for the following class counts
class_counts=( 10 )

for count in "${class_counts[@]}"
do
  echo "Running test with class count: $count"
  CONN="postgres://meteor:meteor@127.0.0.1/meteor_test" CHANNEL="package_test" DEBUG=0 STATS=1 CLASS_COUNT=$count node_modules/nodeunit/bin/nodeunit test/index.js >> oot.log
done

