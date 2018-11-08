#!/bin/sh

echo       Running $0
echo *-*-*-*-*-*-*-*-*-*-*-*-*-*

mvn -P release deploy -P extra-tags -DskipTests=true -B -V -s travis-settings.xml
