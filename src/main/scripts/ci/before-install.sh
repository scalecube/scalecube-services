#!/usr/bin/env bash

DIRNAME=$(dirname $0)
BEFORE_INSTALL_EXEC_FILES=$(find $DIRNAME -name 'before-install-*.sh')

echo       Running $0
echo *-*-*-*-*-*-*-*-*-*-*-*-*-*

echo logging to docker image repository:
echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin

# get latest version of codacy reporter from sonatype
latest=$(curl "https://oss.sonatype.org/service/local/repositories/releases/content/com/codacy/codacy-coverage-reporter/maven-metadata.xml" | xpath -e "/metadata/versioning/release/text()")

echo Downloading latest version $latest of codacy reporter from sonatype 
# download latest assembly jar 
mvn -B -q dependency:get dependency:copy \
   -DoutputDirectory=$HOME \
   -DoutputAbsoluteArtifactFilename=true \
   -Dmdep.stripVersion=true \
   -DrepoUrl=https://oss.sonatype.org/service/local/repositories/releases/content/ \
   -Dartifact=com.codacy:codacy-coverage-reporter:$latest:jar:assembly

echo local file md5sum:   
md5sum ~/codacy-coverage-reporter-assembly.jar
echo remote file md5sum:
curl "https://oss.sonatype.org/service/local/repositories/releases/content/com/codacy/codacy-coverage-reporter/$latest/codacy-coverage-reporter-$latest-assembly.jar.md5"

# extends before-install.sh
for script_file in $BEFORE_INSTALL_EXEC_FILES; do
    . $script_file
done

