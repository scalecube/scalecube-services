#!/bin/bash

mvn -P release release:prepare release:perform -DautoVersionSubmodules=true -DscmCommentPrefix='[skip ci] [maven-release-plugin]' -DskipTests=true -B -V -s travis-settings.xml
git checkout develop
git merge --ff-only master && git push origin develop
