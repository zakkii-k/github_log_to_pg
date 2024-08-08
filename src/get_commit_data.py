import json
import os
import tqdm
import time
import argparse
from Classes import ApiSetting

def parse_arguments():
    parser = argparse.ArgumentParser(description='Process some parameters.')
    current_directory = os.path.dirname(os.path.abspath(__file__))
    parser.add_argument('-o', '--owner_name', type=str, default='apache', help='The name of the owner')
    parser.add_argument('-r', '--repo_name', type=str, default='spark', help='The name of the repository')
    parser.add_argument('-d', '--out_dir', type=str, default=current_directory, help='The output directory')
    return parser.parse_args()

def get_total_pages(api_setting, per_page=100):
    response = api_setting.get_data("commits", with_json=False, params={'per_page': per_page})

    link_header = response.headers.get('Link', None)
    if link_header:
        # get last page number
        links = link_header.split(',')
        for link in links:
            if 'rel="last"' in link:
                last_page_url = link.split(';')[0].strip('<> ')
                last_page_number = last_page_url.split('page=')[-1]
                return int(last_page_number)
    return 1 # if there is only one page

def get_commits(api_setting):
    total_pages = get_total_pages(api_setting)
    print(f"Total pages: {total_pages}")
    commits = []
    for page in tqdm.tqdm(range(1, total_pages+1)):
        response = api_setting.get_data("commits?page=" + str(page), with_json=False)
        if response.status_code == 200:
            commits.extend(response.json())
        else:
            print(f"Error[page: {page}]: {response.status_code}")
            break
        time.sleep(0.5)
    return commits

def write_as_json(data, file_name):
    print(f"Writing data to {file_name}")
    with open(file_name, 'w') as file:
        json.dump(data, file, indent=4)


def get_commit(api_setting, sha_list):
    commits = {}
    for sha in tqdm.tqdm(sha_list):
        response = api_setting.get_data("commits/" + sha, with_json=False)
        if response.status_code == 200:
            commits[sha] = response.json()
        else:
            print(f"Error[sha: {sha}]: {response.status_code}")
        time.sleep(0.5)
    return commits

def main(api_setting, out_dir):
    commits = get_commits(api_setting)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    write_as_json(commits, os.path.join(out_dir, 'commits.json'))
    sha_list = [commit['sha'] for commit in commits]
    commit_details = get_commit(api_setting, sha_list)
    write_as_json(commit_details, os.path.join(out_dir, 'commit_details.json'))



if __name__ == '__main__':
    args = parse_arguments()
    api_setting = ApiSetting(args.owner_name, args.repo_name)
    main(api_setting, args.out_dir)