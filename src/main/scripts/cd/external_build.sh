#!/usr/bin/env python3


import requests
from urllib.parse import urljoin
import json
import time
import sys
import os

class TravisBuilds:
    """
    This class provides requests to builds and check their statuses
    """
    travis_api_url = 'https://api.travis-ci.com/'
    build_id = None

    def __init__(self, repo_name, auth_token):
        self.headers = {'Content-Type': 'application/json',
                        'Accept': 'application/json',
                        'Travis-API-Version': '3',
                        'Authorization': 'token {}'.format(auth_token)
                        }
        self.repo_name = repo_name

    def start_build(self):
        data = {"request": {
            "branch": "master"
        }}
        url = urljoin(self.travis_api_url,
                      'repo/{}/requests'.format(self.repo_name))
        response = requests.post(url=url, data=json.dumps(data), headers=self.headers)
        if response.status_code == 202:
            self.build_id = self.get_build_id(response.json()["request"]["id"])
            print(self.build_id)
        return True

    def get_build_id(self, request_id):
        time.sleep(10)
        url = urljoin(self.travis_api_url,
                      'repo/{}/request/{}'.format(self.repo_name, request_id))
        response = requests.get(url=url, headers=self.headers)
        return response.json()["builds"][0]['id']

    def wait_for_build_result(self):
        attempts = 0
        tests_minutes = int(os.getenv('TESTS_MINUTES'))
        while attempts < tests_minutes:
            url = urljoin(self.travis_api_url, 'build/{}'.format(self.build_id))
            response = requests.get(url=url, headers=self.headers)
            if response.json()['state'] == "passed":
                return True
            else:
                print("External build is running {} minutes".format(attempts))
                time.sleep(60)
                attempts += 1
        return False


if __name__ == '__main__':
    external_build = os.getenv('TRIGGER_EXTERNAL_CI', '')
    if external_build:
        travis = TravisBuilds(external_build, os.getenv('TRAVIS_AUTH_TOKEN'))
        build = travis.start_build()
        result = travis.wait_for_build_result()
        if result:
            sys.exit(0)
        sys.exit(1)
    sys.exit(0)
