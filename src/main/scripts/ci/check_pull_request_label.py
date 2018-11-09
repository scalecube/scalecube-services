import os
from github import Github


cloud_label = os.getenv("CLOUD_DEPLOY_LABEL", None)
pull_request_number = os.getenv("TRAVIS_PULL_REQUEST", None)
github_access_token = os.getenv("GITHUBTOKEN", None)
organization = os.getenv("ORGANIZATION")
repo_name = os.getenv("REPO_NAME")

def check_label_in_pull_request(organization, repo_name, cloud_label, pull_request_number):
    try:
        github = Github(github_access_token)
        org = github.get_organization(organization)
        check_repo = org.get_repo(repo_name)
        check_pull = check_repo.get_pull(int(pull_request_number))
        for label in check_pull.raw_data['labels']:
            if label['name'] == cloud_label:
                print "exist"
    except Exception:
        print "not exist"

if __name__ == "__main__":
    check_label_in_pull_request(organization, repo_name, cloud_label, pull_request_number)
