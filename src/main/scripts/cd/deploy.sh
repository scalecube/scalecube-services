#!/bin/bash


mvn -P release release:prepare release:perform -DautoVersionSubmodules=true -DscmCommentPrefix="$TRAVIS_COMMIT_MESSAGE [skip ci]" -DskipTests=true -B -V -s travis-settings.xml

git checkout develop
git merge --ff-only master && git push origin develop

curl -d '{"title": "Prepare new release","head": "develop","base": "master"}' https://api.github.com/repos/$TRAVIS_REPO_SLUG/pulls?access_token=$GITTOKEN