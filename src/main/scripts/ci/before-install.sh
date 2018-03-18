#!/usr/bin/env bash


sudo apt-get install libxml-xpath-perl
# get latest version of codacy reporter from sonatype
latest=$(curl "https://oss.sonatype.org/service/local/repositories/releases/content/com/codacy/codacy-coverage-reporter/maven-metadata.xml" | xpath -e "/metadata/versioning/release/text()")

# download laterst assembly jar 
mvn dependency:get dependency:copy \
   -DoutputDirectory=~ \
   -DoutputAbsoluteArtifactFilename=true \
   -Dmdep.stripVersion=true \
   -DrepoUrl=https://oss.sonatype.org/service/local/repositories/releases/content/ \
   -Dartifact=com.codacy:codacy-coverage-reporter:$latest:jar:assembly

echo local file md5sum:   
md5sum ~/codacy-coverage-reporter-assembly.jar
echo remote file md5sum:
curl "https://oss.sonatype.org/service/local/repositories/releases/content/com/codacy/codacy-coverage-reporter/$latest/codacy-coverage-reporter-$latest-assembly.jar.md5"

