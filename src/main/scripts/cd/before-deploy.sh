#!/usr/bin/env bash

echo       Running $0
echo *-*-*-*-*-*-*-*-*-*-*-*-*-*

function decryptsecrets {
	echo   decrypting secrets
	echo *-*-*-*-*-*-*-*-*-*-*-*
	mkdir -p ~/tmp
	openssl aes-256-cbc -K $encrypted_SOME_key -iv $encrypted_SOME_iv -in $TRAVIS_BUILD_DIR/src/main/scripts/cd/secrets.tar.enc -out ~/tmp/secrets.tar -d
	md5sum ~/tmp/secrets.tar
	tar -xvf ~/tmp/secrets.tar -C  ~/.ssh
	shred -z -u ~/tmp/secrets.tar    
}

function importpgp {
	echo   importing pgp secret
	echo *-*-*-*-*-*-*-*-*-*-*-*
	eval $(gpg-agent --daemon --batch)
    gpg --batch --passphrase $GPG_PASSPHRASE --import  ~/.ssh/codesigning.asc
    shred -z -u ~/.ssh/codesigning.asc
}

function setupssh {
	echo   importing ssh secret
	echo *-*-*-*-*-*-*-*-*-*-*-*
    chmod 400 ~/.ssh/id_rsa
    touch ~/.ssh/config

    echo "Host github.com" >> $HOME/.ssh/config
    echo "    IdentityFile $HOME/.ssh/id_rsa" >> $HOME/.ssh/config
    echo "    StrictHostKeyChecking no" >> $HOME/.ssh/config
	
	eval "$(ssh-agent -s)"
	ssh-add ~/.ssh/id_rsa
	ssh -T git@github.com | true
}
	
function setupgit {
	echo   setting git up
	echo *-*-*-*-*-*-*-*-*-*-*-*
    git remote set-url origin git@github.com:$TRAVIS_REPO_SLUG.git
	git config --global user.email "io.scalecube.ci@gmail.com"
    git config --global user.name "io-scalecube-ci"
	git checkout -B $TRAVIS_BRANCH | true
}

function deployment {
  if [ "$TRAVIS_PULL_REQUEST" == 'false' ] &&  [ "$TRAVIS_BRANCH" = 'master' ]  || [ "$TRAVIS_BRANCH" = 'develop' ]; then
	echo     deployment
	echo *-*-*-*-*-*-*-*-*-*-*-*
    decryptsecrets
    importpgp
    setupssh
    setupgit
  fi
}

deployment
