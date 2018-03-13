#!/bin/bash

mvn -P release release:prepare -DautoVersionSubmodules=true -DscmCommentPrefix='[skip ci] [maven-release-plugin]' -DskipTests=true -Dmaven.javadoc.skip=true -B -V -s travis-settings.xml
mvn release:perform