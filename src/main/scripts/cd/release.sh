#!/usr/bin/env sh

DIRNAME=$(dirname $0)
RELEASE_EXEC_FILES=$(find $DIRNAME -name 'release-*.sh')

echo       Running $0
echo *-*-*-*-*-*-*-*-*-*-*-*-*-*

commit_to_develop() { 
 git fetch
 git branch -r
 git checkout -B develop 
 git rebase $TRAVIS_BRANCH
 git commit --amend -m "++++ Prepare for next development iteration build: $TRAVIS_BUILD_NUMBER ++++"
 git push origin develop
}

function check_next_version {
  export NEXT_VERSION=$(echo $TRAVIS_COMMIT_MESSAGE | grep -E -o '[0-9]+\.[0-9]+\.[0-9]+-SNAPSHOT')
  if [ -n "$NEXT_VERSION" ] ; then
    export MVN_NEXT_VERSION=-DdevelopmentVersion=$NEXT_VERSION
  fi
}

function check_tag_for_rc {
  export VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
  if [ -n "$TRAVIS_TAG" ] ; then
    RC_VER=$(echo $TRAVIS_TAG | grep -E -o 'RC-?[0-9]+')
    RC_PREPARE=$(echo $TRAVIS_TAG | grep -o -i 'prepare')
    if [ -n "$RC_VER" -a -n "$RC_PREPARE" ] ; then
      export NEW_RC_VERSION=$(echo $VERSION | sed  "s/SNAPSHOT/$RC_VER/g")
      echo Release candidate: $NEW_RC_VERSION
      echo *-*-*-*-*-*-*-*-*-*-*-*
      export MVN_RELEASE_VERSION=-DreleaseVersion=$NEW_RC_VERSION
      if [ -n "$MVN_NEXT_VERSION" ] ; then
        export MVN_NEXT_VERSION=-DdevelopmentVersion=$VERSION;
      fi
    fi
  fi
}

check_next_version
check_tag_for_rc

mvn -P release -Darguments=-DskipTests release:prepare release:perform $MVN_RELEASE_VERSION $MVN_NEXT_VERSION -DautoVersionSubmodules=true -DscmCommentPrefix="$TRAVIS_COMMIT_MESSAGE [skip ci] " -B -V -s travis-settings.xml

mvn clean

if [ -z "$NEW_RC_VERSION" ]; then 
  commit_to_develop
fi

# extends release.sh
for script_file in $RELEASE_EXEC_FILES; do
    . $script_file
done
