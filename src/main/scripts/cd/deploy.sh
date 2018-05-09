#!/bin/sh


commit_to_develop() { 
 git fetch
 git branch -r
 git checkout -B develop 
 git rebase master
 git commit --amend -m "++++ Prepare for next development iteration build: $TRAVIS_BUILD_NUMBER ++++"
 git push origin develop && curl -u "$GITHUBUSER:$GITHUBTOKEN" -d '{"title": "Prepare new release","head": "develop","base": "master"}' https://api.github.com/repos/$TRAVIS_REPO_SLUG/pulls?access_token=$GITTOKEN

}

mvn -P release release:prepare release:perform -DautoVersionSubmodules=true -DscmCommentPrefix="$TRAVIS_COMMIT_MESSAGE [skip ci] " -DskipTests=true -B -V -s travis-settings.xml


mvn clean
commit_to_develop
