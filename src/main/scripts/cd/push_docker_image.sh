#!/usr/bin/env bash
pip install --user -r requirements.txt
label_exist=$(python scripts/check_pull_request_label.py)
if [ ! "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$label_exist" == "exist" ]; then
    mvn clean install -DskipTests
fi
