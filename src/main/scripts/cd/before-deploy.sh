#!/usr/bin/env bash


if [ "$TRAVIS_PULL_REQUEST" == 'false' ] &&  [ "$TRAVIS_BRANCH" = 'master' ]  || [ "$TRAVIS_BRANCH" = 'develop' ]; then
	pushd src/main/scripts/cd
	mkdir ~/tmp
	openssl aes-256-cbc -K $encrypted_d19fb18b4b9d_key -iv $encrypted_d19fb18b4b9d_iv -in codesigning.asc.enc -out  ~/tmp/codesigning.asc -d
    gpg --fast-import ~/tmp/codesigning.asc
    shred -z -u ~/tmp/codesigning.asc
    popd    
fi