#!/bin/sh

echo       Running $0
echo *-*-*-*-*-*-*-*-*-*-*-*-*-*

mvn -P release deploy -DskipTests=true -B -V -s travis-settings.xml
pip install --user -r requirements.txt
$(dirname $0)/external_build.sh
