#!/bin/bash
declare -a gws=("localhost")
declare -a services=("localhost")
declare -a clients=("localhost")

CERT_PATH=/home/sergiiripa/certs/cloud_cert
RESULTS_ROOT=/path/to/tests

for addr in ${gws[@]}
do
    mkdir -p $RESULTS_ROOT/gateway/$addr
    scp -r -i $CERT_PATH ubuntu@$addr:/tmp/reports $RESULTS_ROOT/gateway/$addr
done

CLIENT_REPORTS_DIR=/tmp/reports
for addr in ${clients[@]}
do
    mkdir -p $RESULTS_ROOT/client/$addr
    scp -r -i $CERT_PATH ubuntu@$addr:$CLIENT_REPORTS_DIR $RESULTS_ROOT/client/$addr
done
