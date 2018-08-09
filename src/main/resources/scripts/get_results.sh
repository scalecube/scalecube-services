#!/bin/bash
declare -a gws=("35.180.126.189")
declare -a services=("35.180.83.214")
declare -a clients=("35.180.28.113" "35.180.85.27" "35.180.101.3" "52.47.173.231")

CERT_PATH=/home/sergiiripa/certs/cloud_cert
RESULTS_ROOT=/home/sergiiripa/tmp/testReports/requestStream-1

for addr in ${gws[@]}
do
    mkdir -p $RESULTS_ROOT/gateway
    scp -r -i $CERT_PATH ubuntu@$addr:/tmp/reports $RESULTS_ROOT/gateway
done

CLIENT_REPORTS_DIR=/tmp/reports
for addr in ${clients[@]}
do
    mkdir -p $RESULTS_ROOT/client/$addr
    scp -r -i $CERT_PATH ubuntu@$addr:$CLIENT_REPORTS_DIR $RESULTS_ROOT/client/$addr
done
