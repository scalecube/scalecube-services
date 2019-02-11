#!/usr/bin/env sh

DIRNAME=$(dirname $0)
RELEASE_EXEC_FILES=$(find $DIRNAME -name 'release-*.sh')

echo       Running $0
echo *-*-*-*-*-*-*-*-*-*-*-*-*-*

commit_to_develop() { 
 git fetch
 git branch -r
 git checkout -B develop 
 git rebase master
 git commit --amend -m "++++ Prepare for next development iteration build: $TRAVIS_BUILD_NUMBER ++++"
 git push origin develop
}

mvn -P release -Darguments=-DskipTests release:prepare release:perform -DautoVersionSubmodules=true -DscmCommentPrefix="$TRAVIS_COMMIT_MESSAGE [skip ci] " -B -V -s travis-settings.xml

mvn clean
commit_to_develop

# extends release.sh
for script_file in $RELEASE_EXEC_FILES; do
    . $script_file
done
