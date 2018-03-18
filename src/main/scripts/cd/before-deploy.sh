#!/usr/bin/env bash


if [ "$TRAVIS_PULL_REQUEST" == 'false' ] &&  [ "$TRAVIS_BRANCH" = 'master' ]  || [ "$TRAVIS_BRANCH" = 'develop' ]; then
    git remote set-url origin git@github.com:$TRAVIS_REPO_SLUG.git
	git config --global user.email "io.scalecube.ci@gmail.com"
    git config --global user.name "io-scalecube-ci"
    git config --global commit.gpgSign false
	pushd src/main/scripts/cd
	mkdir ~/tmp
	openssl aes-256-cbc -K $encrypted_d19fb18b4b9d_key -iv $encrypted_d19fb18b4b9d_iv -in secrets.tar.enc -out ~/tmp/secrets.tar -d
	md5sum secrets.tar
	tar -xvf ~/tmp/secrets.tar -C  ~/tmp
	md5sum ~/tmp/*
    gpg --fast-import ~/tmp/codesigning.asc
    md5sum ~/tmp/id_rsa
    chmod 600 ~/tmp/id_rsa
    shred -z -u ~/tmp/codesigning.asc
	eval "$(ssh-agent -s)"
	ssh-add ~/tmp/id_rsa
	ssh -T git@github.com | true
	git fetch
    popd    
fi

sudo apt-get install libxml-xpath-perl
# get latest version of codacy reporter from sonatype
latest=$(curl "https://oss.sonatype.org/service/local/repositories/releases/content/com/codacy/codacy-coverage-reporter/maven-metadata.xml" | xpath -e "/metadata/versioning/release/text()") +  | xpath -e "/metadata/versioning/release/text()")
# download laterst assembly jar 
mvn dependency:get dependency:copy \
   -DoutputDirectory=~ \
   -DoutputAbsoluteArtifactFilename=true \
   -Dmdep.stripVersion=true \
   -DrepoUrl=https://oss.sonatype.org/service/local/repositories/releases/content/ \
   -Dartifact=com.codacy:codacy-coverage-reporter:$latest:jar:assembly
   
md5sum ~/codacy-coverage-reporter-assembly.jar
