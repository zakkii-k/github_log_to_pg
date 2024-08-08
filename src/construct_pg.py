import json
import os
import argparse
import tqdm
from Classes import Nodes, Edges, CommitNode, FileNode, Files, ApiSetting

def parse_arguments():
    parser = argparse.ArgumentParser(description='Process some parameters.')
    current_directory = os.path.dirname(os.path.abspath(__file__))
    parser.add_argument('-o', '--owner_name', type=str, default='apache', help='The name of the owner')
    parser.add_argument('-r', '--repo_name', type=str, default='spark', help='The name of the repository')
    parser.add_argument('-j', '--json_path', type=str, default=os.path.join(current_directory,'commit_details.json'), help='The output directory')
    parser.add_argument('-d', '--out_dir', type=str, default=current_directory, help='The output directory')
    return parser.parse_args()

        
def load_json(file_name):
    with open(file_name, 'rb') as file:
        return json.load(file)

def write_as_json(data, file_name):
    print(f"Writing data to {file_name}")
    # もしディレクトリが存在しない場合は作成
    os.makedirs(os.path.dirname(file_name), exist_ok=True)
    with open(file_name, 'w') as file:
        json.dump(data, file, indent=4)


def main(api_setting, json_path, out_dir):
    nodes = Nodes()
    edges = Edges()
    commits = load_json(json_path)
    files = Files(api_setting)
    print("add commit nodes")
    for sha, commit in tqdm.tqdm(commits.items()):
        commit_data = CommitNode(commit)
        nodes.add_node(commit_data)
    print("add commit-commit edges and commit-file edges")
    for sha, commit in tqdm.tqdm(commits.items()):
        dst = nodes.get_id(sha)
        for parent in commit['parents']:
            src = nodes.get_id(parent['sha'])
            edges.add_edge({'src': src, 'dst': dst, 'label': ['isParentOf'], 'property': {'date': commit['commit']['author']['date']}})
        
        for file in commit['files']:
            file_data = FileNode(file, sha)
            nodes.add_node(file_data)
            try:
                file_data.get_id()
            except:
                raise Exception(f"Error: {file_data.get_data()}")
            edges.add_edge({'src': nodes.get_id(file_data.get_hash()), 'dst': dst, 'label': [file_data.get_status(), 'commit'], 'property': {}})
            files.add_file(sha, file_data, commit['commit']['author']['date'])

    write_as_json(list(nodes.get_nodes().values()), os.path.join(out_dir, 'nodes.json'))
    files.write_as_json(os.path.join(out_dir, 'files.json'))

    print("connect file nodes")
    edges = files.connect_files(edges)
            
    write_as_json(list(edges.get_edges().values()), os.path.join(out_dir, 'edges.json'))
    return


if __name__ == '__main__':
    args = parse_arguments()
    api_setting = ApiSetting(args.owner_name, args.repo_name)
    main(api_setting, args.json_path, args.out_dir)

