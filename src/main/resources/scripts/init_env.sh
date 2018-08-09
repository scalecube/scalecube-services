#!/bin/bash
declare -a gws=("35.180.126.189")
declare -a services=("35.180.83.214")
declare -a clients=("35.180.28.113" "35.180.85.27" "35.180.101.3" "52.47.173.231")

CERT_PATH=/home/sergiiripa/certs/cloud_cert
GW_VERSION=origin/feature/MPA-5321-rsocket-microbechmarks
GATEWAY_PATH=/home/sergiiripa/work/code/scalecube-gateway

#cd $GATEWAY_PATH
#git fetch
#git checkout $GW_VERSION
#mvn clean install -DskipTests
#cd -

for addr in ${gws[@]}
do
    echo "####### Setting up gateway: #######"
    echo "$addr"
    ssh -oStrictHostKeyChecking=no -i $CERT_PATH ubuntu@$addr 'sudo rm -rf /tmp/*' 
    scp -i $CERT_PATH $GATEWAY_PATH/RSocketWebsocketRunner/target/scalecube-gateway-rsocket-websocket-runner-*-shaded.jar ubuntu@$addr:/tmp/gw.jar
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
