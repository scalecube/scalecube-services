#!/bin/sh

echo       Running $0
echo *-*-*-*-*-*-*-*-*-*-*-*-*-*

mvn -P release deploy -DskipTests=true -B -V -s travis-settings.xml
apt update && apt install -y python3-pip && pip3 install -r requirements.txt
$(dirname $0)/external_build.sh
