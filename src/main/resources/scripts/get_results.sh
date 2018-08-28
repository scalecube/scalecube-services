#!/bin/bash
declare -a gws=("ec2-35-180-27-71.eu-west-3.compute.amazonaws.com")
declare -a services=("ip3" "ip4")
declare -a clients=("ip5" "ip6" "ip7" "ip8")

CERT_PATH=/home/serhiihabryiel/Downloads/cloud_cert
RESULTS_ROOT=/home/serhiihabryiel/Snapshots/new/4cpu/StandaloneRequestStreamMicrobenchmark

for addr in ${gws[@]}
do
    scp -r -i $CERT_PATH ubuntu@$addr:/tmp/reports $RESULTS_ROOT
done
#
#for addr in ${clients[@]}
#do
#    mkdir -p $RESULTS_ROOT/client/$addr
#    scp -r -i $CERT_PATH ubuntu@$addr:/tmp/reports $RESULTS_ROOT/client/$addr
#done
#
#for addr in ${services[@]}
#do
#    mkdir -p $RESULTS_ROOT/client/$addr
#    scp -r -i $CERT_PATH ubuntu@$addr:/tmp/reports $RESULTS_ROOT/services/$addr
#done
