import hashlib
from bidict import bidict
import os
import requests
import time
import tqdm
from datetime import datetime, timezone, timedelta
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class UniqueIdGenerator:
    def __init__(self):
        self.generated_ids_dict = bidict()

    def sha256(self, input_string):
        return hashlib.sha256(input_string.encode('utf-8')).digest()

    def bytes_to_long(self, bytes):
        return int.from_bytes(bytes[:8], byteorder='big')
    
    def add_id(self, input_string, id):
        self.generated_ids_dict[id] = input_string

    def generate_unique_id(self, input_string):
        try:
            hash_bytes = self.sha256(input_string)
            unique_id = self.bytes_to_long(hash_bytes)
            iteration = 0

            # If the ID is already present, recalculate using different parts of the hash
            while unique_id in self.generated_ids_dict:
                iteration += 1
                # Change the part of the hash used or use another hash function
                if iteration < 4:
                    unique_id = self.bytes_to_long(hash_bytes[iteration*8:(iteration+1)*8])
                else:
                    # If all parts of the hash are used, generate a new hash with a salt
                    salted_input = input_string + str(iteration)
                    hash_bytes = self.sha256(salted_input)
                    unique_id = self.bytes_to_long(hash_bytes)
            return unique_id
        except Exception as e:
            print(f"Error generating unique ID for input: {input_string}")
            print(f"Exception: {e}")
            raise
    def assign_unique_id(self, input_string):
        unique_id = self.generate_unique_id(input_string)
        self.add_id(input_string, unique_id)
        return unique_id

class Node:
    def __init__(self):
        pass

    def __repr__(self):
        return "Node()"
    
    def get_nested_value(self, d, key_path):
        keys = key_path.split('.')
        for key in keys:
            d = d[key]
        return d

    def extract_and_rename(self, data, key_mapping):
        result = {}
        for old_key, new_key in key_mapping:
            try:
                result[new_key] = self.get_nested_value(data, old_key)
            except KeyError:
                continue
        return result
    
    def get_data(self):
        pass

    def get_hash(self):
        pass
    
    def assign_id(self, id):
        pass

    def to_dict(self):
        return self.__dict__

class CommitNode(Node):
    extract_keys_for_commit = [
        ('sha', 'sha'), 
        ('commit.author.name', 'author_name'), 
        ('commit.author.email', 'author_email'), 
        ('commit.author.date', 'author_date'), 
        ('commit.committer.name', 'committer_name'), 
        ('commit.committer.email', 'committer_email'), 
        ('commit.committer.date', 'committer_date'),
        ('commit.message', 'message'),
        ('url', 'url'),
        ('stats.total', 'total'),
        ('stats.additions', 'additions'),
        ('stats.deletions', 'deletions'),
        ]
    def __init__(self, commit, need_extract=True):
        self.commit = commit
        if not need_extract:
            self.data = commit
            return 
        self.data = self.extract_and_rename(commit, self.extract_keys_for_commit)
        if len(commit['parents']) > 1:
            self.data['label'] = ['merge', 'commit']
        else:
            self.data['label'] = ['commit']
    
    def get_data(self):
        return self.data
    
    def get_hash(self):
        return self.data['sha']
    
    def assign_id(self, id):
        self.data['id'] = id
    
    def to_dict(self):
        return self.data
    

class FileNode(Node):    
    extract_keys_for_file = [
        ('sha', 'sha'), 
        ('filename', 'filename'),
        ('status', 'status'),
        ('additions', 'additions'),
        ('deletions', 'deletions'),
        ('changes', 'changes'),
        ('blob_url', 'blob_url'),
        ('patch', 'patch'),
        ('previous_filename', 'previous_filename'),
        ]
    def __init__(self, file, need_extract=True):
        self.file = file
        if not need_extract:
            self.data = file
            return
        self.data = self.extract_and_rename(file, self.extract_keys_for_file)
        self.data['directory'] = os.path.dirname(self.data['filename'])
        extension = os.path.splitext(self.data['filename'])[1]
        self.data['label'] = ['file', extension]
    
    def __init__(self, file, sha, need_extract=True):
        self.file = file
        self.commit_sha = sha
        if not need_extract:
            self.data = file
            return
        self.data = self.extract_and_rename(file, self.extract_keys_for_file)
        self.data['directory'] = os.path.dirname(self.data['filename'])
        extension = os.path.splitext(self.data['filename'])[1]
        self.data['label'] = ['file', extension]
    
    def get_data(self):
        return self.data
    
    def get_hash(self):
        return str(self.commit_sha)+str(self.data['sha'])+self.data['blob_url']  
    
    def get_name(self):
        return self.data['filename']
    
    def get_status(self):
        return self.data['status']
    
    def assign_id(self, id):    
        self.data['id'] = id
        # print(f"ID: {id} is assigned to {self.data['sha']}")
    
    def get_id(self):
        try:
            return self.data['id']
        except KeyError:
            print(f"ID is not assigned to {self.data['filename']}")
            print(f"Data: {self.data}")
            raise
    
    def to_dict(self):
        return self.data
        

class Nodes:
    def __init__(self):
        self.nodes = {}
        self.sha_id_map = bidict()
        self.unique_id_generator = UniqueIdGenerator()

    def add_node(self, node):
        if node.get_hash() in self.sha_id_map:
            print(f"Node {node.get_hash()} is already recorded.")
            print(f"ID is {self.sha_id_map[node.get_hash()]}")
            return
        id = self.unique_id_generator.generate_unique_id(node.get_hash())
        self.sha_id_map[node.get_hash()] = id
        node_data = node.to_dict().copy()
        label = node_data.pop('label')
        
        self.nodes[node.get_hash()] = {
            "id": id,
            "label": label,
            "property": node_data
        }
        node.assign_id(id)

    def add_nodes(self, nodes):
        for node in nodes:
            self.add_node(node)

    def get_nodes(self):
        return self.nodes
    
    def get_id(self, sha):
        if sha in self.sha_id_map:
            return self.sha_id_map[sha]
        else:
            id = self.unique_id_generator.generate_unique_id(sha)
            self.sha_id_map[sha] = id
            return id
    def get_sha(self, id):
        if id in self.sha_id_map.inverse:
            return self.sha_id_map.inverse[id]
        else:
            return None

    def load_nodes(self, filepath):
        try:
            with open(filepath, 'r') as json_file:
                print(f"Loading nodes from {filepath}")
                data = json.load(json_file)
                for node_data in data:
                    if 'commit' in node_data['label']:
                        node = CommitNode(node_data, need_extract=False)
                    else:
                        node = FileNode(node_data, need_extract=False)
                    
                    self.add_node(node)
        except Exception as e:
            print(f"Error loading JSON from {filepath}")
            print(f"Exception: {e}")

class Edges:
    def __init__(self):
        self.edges = {}
        self.src_dict = {}
        self.dst_dict = {}
        self.unique_id_generator = UniqueIdGenerator()

    def add_edge(self, edge):
        if (edge['src'], edge['dst'], tuple(edge['label'])) in self.edges:
            return
        id = self.unique_id_generator.generate_unique_id(str(edge['src'])+str(edge['dst'])+''.join(edge['label']))
        edge['id'] = id
        self.edges[(edge['src'], edge['dst'], tuple(edge['label']))] = edge
        if edge['src'] in self.src_dict:
            self.src_dict[edge['src']].append(edge['dst'])
        else:
            self.src_dict[edge['src']] = [edge['dst']]
        if edge['dst'] in self.dst_dict:
            self.dst_dict[edge['dst']].append(edge['src'])
        else:
            self.dst_dict[edge['dst']] = [edge['src']]

    def add_edges(self, edges):
        for edge in edges:
            self.add_edge(edge)

    def get_edges(self):
        return self.edges
    
    def get_dsts(self, src_id):
        if src_id in self.src_dict:
            return self.src_dict[src_id]
        else:
            return []
    
    def get_srcs(self, dst_id):
        if dst_id in self.dst_dict:
            return self.dst_dict[dst_id]
        else:
            return []
        
class Files:
    def __init__(self, api_setting):
        self.files = {} # filename: [FileNode]  
        self.file_commit_dict = {} # filename: [(commit_sha, commit_date)] commit_shaは新しい順になっている．
        self.api_setting = api_setting

    def add_file(self, sha, file, date):
        date_obj = datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ")
        if file.get_name() not in self.files:
            self.files[file.get_name()] = {}
            self.files[file.get_name()][sha] = file
            self.file_commit_dict[file.get_name()] = [(sha, date_obj)]
        else:
            self.files[file.get_name()][sha] = file
            self.file_commit_dict[file.get_name()].append((sha, date_obj))
    
    def get_file(self, filename, sha):
        return self.files[filename][sha]

    def connect_files(self, edges):
        for filename, commit_sha in tqdm.tqdm(self.file_commit_dict.items()):
            if len(commit_sha) == 1:
                continue
            ind = 0
            # コミットの時系列を新しい順に
            commit_sha.sort(key=lambda x: x[1], reverse=True)
            while ind < len(commit_sha)-1:
                current_sha = commit_sha[ind][0]
                current_file = self.files[filename][current_sha]
                if current_file.get_status() == 'renamed' and current_file.get_name() == filename:
                    ind += 1
                    continue
                before_sha = commit_sha[ind+1][0]
                before_file = self.files[filename][before_sha]
                edges.add_edge({'src': before_file.get_id(), 'dst': current_file.get_id(), 'label': ['isPreviousVersionOf', current_file.get_status()], 'property': {'date': commit_sha[ind][1]}})
                ind += 1
        return edges
    
    def load_files(self, filepath):
        try:
            with open(filepath, 'r') as json_file:
                print(f"Loading files from {filepath}")
                data = json.load(json_file)
                
                # 復元したデータを Files クラスのインスタンスに設定する
                for filename, file_data in data['files'].items():
                    self.files[filename] = {}
                    for sha, file_info in file_data.items():
                        file_node = FileNode(file_info, need_extract=False)
                        self.files[filename][sha] = file_node
                
                # ファイルのコミット情報を復元する
                self.file_commit_dict = data['file_commit_dict']
        
        except Exception as e:
            print(f"Error loading JSON from {filepath}")
            print(f"Exception: {e}")

    def to_dict(self):
        # Convert the files and file_commit_dict to a dictionary
        return {
            "files": {
                filename: {sha: file.get_data() for sha, file in files.items()}
                for filename, files in self.files.items()
            },
            "file_commit_dict": self.file_commit_dict
        }

    def write_as_json(self, filepath):
        try:
            # Convert the dictionary to a JSON string and write it to a file
            with open(filepath, 'w') as json_file:
                print(f"Writing files to {filepath}")
                json.dump(self.to_dict(), json_file, indent=4, cls=DateTimeEncoder)
        except Exception as e:
            print(f"Error writing JSON to {filepath}")
            print(f"Exception: {e}")
            
                
class ApiSetting:
    def __init__(self, owner_name, repo_name):
        self.base_url = f"https://api.github.com/repos/{owner_name}/{repo_name}/"
        self.token = os.getenv('GITHUB_TOKEN')
        if not self.token:
            raise ValueError("GITHUB_TOKEN is not set")
        self.headers = {"Authorization": f"token {self.token}"}
        self.count = 0

        # リトライロジックの設定
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.http = requests.Session()
        self.http.mount("https://", adapter)
        self.http.mount("http://", adapter)
    
    def get_data(self, api_string="", params=None):
        while True:
            try:
                self.count += 1
                print(f"======== API count: {self.count} ========")
                response = self.http.get(self.base_url + api_string, headers=self.headers, params=params)
                
                remaining = int(response.headers.get('X-RateLimit-Remaining', 1))  # デフォルトは1でエラーを避ける
                reset_time = int(response.headers.get('X-RateLimit-Reset', time.time() + 60))  # デフォルトは現在時刻から1分後

                if response.status_code == 200:
                    if remaining == 0:
                        wait_time = max(0, reset_time - time.time())
                        reset_time_jst = datetime.fromtimestamp(reset_time, timezone.utc) + timedelta(hours=9)
                        wait_hours, remainder = divmod(wait_time, 3600)
                        wait_minutes, wait_seconds = divmod(remainder, 60)
                        print(f"Rate limit exceeded. Waiting for {int(wait_hours)} hours, {int(wait_minutes)} minutes, and {int(wait_seconds)} seconds until {reset_time_jst.strftime('%Y-%m-%d %H:%M:%S %Z%z')} (JST).")
                        time.sleep(wait_time + 1)  # 余裕を持って1秒追加
                    return response.json()
                elif response.status_code == 403:
                    print("Rate limit info:", response.headers.get('X-RateLimit-Limit'), remaining)
                    print("Error 403: Access Forbidden. You may have hit a rate limit or the token is invalid.")
                    print(f"Response: {response.text}")
                    if remaining == 0:
                        wait_time = max(0, reset_time - time.time())
                        reset_time_jst = datetime.fromtimestamp(reset_time, timezone.utc) + timedelta(hours=9)
                        wait_hours, remainder = divmod(wait_time, 3600)
                        wait_minutes, wait_seconds = divmod(remainder, 60)
                        print(f"Rate limit exceeded. Waiting for {int(wait_hours)} hours, {int(wait_minutes)} minutes, and {int(wait_seconds)} seconds until {reset_time_jst.strftime('%Y-%m-%d %H:%M:%S %Z%z')} (JST).")
                        time.sleep(wait_time + 1)  # 余裕を持って1秒追加
                        continue  # リトライする
                    return None
                else:
                    print(f"Error {response.status_code}: {response.reason}")
                    print(f"Response: {response.text}")
                    return None
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}")
                time.sleep(5)  # 失敗した場合は5秒待機して再試行