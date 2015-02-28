#!/bin/bash
# Append test suite results to oot.log for the following class counts
class_counts=( 10 )
client_multipliers=( 1 )
update_percents=( 10 )
assigns_per_class=( 80 )

for count in "${class_counts[@]}"
do
for client_multiplier in "${client_multipliers[@]}"
do
for update_percent in "${update_percents[@]}"
do
for assign_per_class in "${assigns_per_class[@]}"
do
	echo "Running test with class count: $count, $client_multiplier instances per class, $update_percent % of scores rows updated, $assign_per_class assignments per class" | tee -a oot.log
	CONN="postgres://meteor:meteor@127.0.0.1/meteor_test" CHANNEL="scores_load" LOAD_TEST=1 DEBUG=0 STATS=1 CLASS_COUNT=$count CLIENT_MULTIPLIER=$client_multiplier PERCENT_TO_UPDATE=$update_percent ASSIGN_PER_CLASS=$assign_per_class node_modules/nodeunit/bin/nodeunit test/index.js | tee -a oot.log
done
done
done
done

