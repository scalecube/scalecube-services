#!/usr/bin/env bash
pip install --user -r requirements.txt
label_exist=$(python src/main/scripts/cd/check_pull_request_label.py)
echo "Travis PULL_REQUEST is $TRAVIS_PULL_REQUEST"
if [ ! "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$label_exist" == "exist" ]; then
    mvn clean install -DskipTests
    LABEL=$(echo $(git rev-parse HEAD) | cut -c1-7)
    docker tag scalecube/scalecube-services-benchmarks:latest scalecube/scalecube-services-benchmarks:$LABEL
    docker tag scalecube/scalecube-services-examples-runner:latest scalecube/scalecube-services-examples-runner:$LABEL
    docker tag scalecube/scalecube-services-gateway-runner:latest scalecube/scalecube-services-gateway-runner:$LABEL
    echo $DOCKER_PASSWORD | docker login --username $DOCKER_USERNAME --password-stdin
    docker push scalecube/scalecube-services-benchmarks:$LABEL
    docker push scalecube/scalecube-services-examples-runner:$LABEL
    docker push scalecube/scalecube-services-gateway-runner:$LABEL 
fi
