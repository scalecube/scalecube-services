#!/usr/bin/env bash

echo Running $0
echo ---------------------
if [ "$TRAVIS_PULL_REQUEST" == 'false' ] &&  [ "$TRAVIS_BRANCH" = 'master' ]  || [ "$TRAVIS_BRANCH" = 'develop' ]; then
	echo     deployment
	echo -----------------
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
