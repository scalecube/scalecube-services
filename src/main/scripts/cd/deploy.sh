#!/bin/bash

mvn -P release release:prepare release:perform -DautoVersionSubmodules=true -DscmCommentPrefix="$TRAVIS_COMMIT_MESSAGE [skip ci] " -DskipTests=true -B -V -s travis-settings.xml


pr_master_to_dev=$(curl -u "$GITHUBUSER:$GITHUBTOKEN" -d '{"title": "Prepare for next development iteration","base": "develop" ,"head":"master"}' https://api.github.com/repos/$TRAVIS_REPO_SLUG/pulls)

prid=$(echo $pr_master_to_dev | jq '.id')

until curl --silent --show-error --fail -XPUT -u "$GITHUBUSER:$GITHUBTOKEN" -d  '{"commit_title":"Prepare for next development iteration"}'  https://api.github.com/repos/$TRAVIS_REPO_SLUG/pulls/$prid/merge
do 
    echo "waiting for successful merge"
	sleep 10
done 

curl -u "$GITHUBUSER:$GITHUBTOKEN" -d '{"title": "Prepare new release","head": "develop","base": "master"}' https://api.github.com/repos/$TRAVIS_REPO_SLUG/pulls?access_token=$GITTOKEN