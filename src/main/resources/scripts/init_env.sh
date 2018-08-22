#!/bin/bash
declare -a gws=("35.180.50.17" "52.47.115.55")
declare -a services=("35.180.125.22" "35.180.73.238")
declare -a clients=("35.180.28.35" "35.180.37.182" "52.47.110.72" "35.180.26.168")

CERT_PATH=/home/serhiihabryiel/Downloads/cloud_cert
GATEWAY_PATH=/home/serhiihabryiel/work/scalecube-gateway

for addr in ${gws[@]}
do
    echo "####### Setting up gateway: #######"
    echo "$addr"
    ssh -oStrictHostKeyChecking=no -i $CERT_PATH ubuntu@$addr 'sudo rm -rf /tmp/*'
    scp -i $CERT_PATH $GATEWAY_PATH/GatewayRunner/target/scalecube-gateway-runner-*-SNAPSHOT-shaded.jar ubuntu@$addr:/tmp/gw.jar
    ssh -oStrictHostKeyChecking=no -i $CERT_PATH ubuntu@$addr 'bash -s' < init_java.sh
done

for addr in ${services[@]}
do
    echo "####### Setting up service: #######"
    echo "$addr"
    ssh -oStrictHostKeyChecking=no -i $CERT_PATH ubuntu@$addr 'sudo rm -rf /tmp/*'
    scp -i $CERT_PATH $GATEWAY_PATH/Examples/target/scalecube-gateway-examples-*-shaded.jar ubuntu@$addr:/tmp/examples.jar
    ssh -oStrictHostKeyChecking=no -i $CERT_PATH ubuntu@$addr 'bash -s' < init_java.sh
done

for addr in ${clients[@]}
do
    echo "####### Setting up client: #######"
    echo "$addr"
    ssh -oStrictHostKeyChecking=no -i $CERT_PATH ubuntu@$addr 'sudo rm -rf /tmp/*'
    scp -i $CERT_PATH $GATEWAY_PATH/Benchmarks/target/scalecube-gateway-benchmarks-*-shaded.jar ubuntu@$addr:/tmp/scenarios.jar
    ssh -oStrictHostKeyChecking=no -i $CERT_PATH ubuntu@$addr 'bash -s' < init_java.sh
done
