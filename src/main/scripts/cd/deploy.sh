#!/bin/sh

echo       Running $0
echo *-*-*-*-*-*-*-*-*-*-*-*-*-*

mvn -P release deploy -DskipTests=true -B -V -s travis-settings.xml
