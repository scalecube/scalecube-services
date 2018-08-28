#!/bin/bash
declare -a gws=("ip1" "ip2")
declare -a services=("ip3" "ip4")
declare -a clients=("ip5" "ip6" "ip7" "ip8")

CERT_PATH=/home/serhiihabryiel/Downloads/cloud_cert
RESULTS_ROOT=/home/serhiihabryiel/Snapshots/new/2cpu_16injectors

for addr in ${gws[@]}
do
    mkdir -p $RESULTS_ROOT/gateway/$addr
    scp -r -i $CERT_PATH ubuntu@$addr:/tmp/reports $RESULTS_ROOT/gateway/$addr
done

for addr in ${clients[@]}
do
    mkdir -p $RESULTS_ROOT/client/$addr
    scp -r -i $CERT_PATH ubuntu@$addr:/tmp/reports $RESULTS_ROOT/client/$addr
done

for addr in ${services[@]}
do
    mkdir -p $RESULTS_ROOT/client/$addr
    scp -r -i $CERT_PATH ubuntu@$addr:/tmp/reports $RESULTS_ROOT/services/$addr
done
