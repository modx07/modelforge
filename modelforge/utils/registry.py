import importlib
import json
import os
import sqlite3

import git
import pandas as pd

from datetime import datetime
from pathlib import Path

from modelforge.utils.repo import check_class, check_repo, get_github_file_url


def relative_path(path1, path2):
    path1 = Path(path1).resolve()
    path2 = Path(path2).resolve()
    return os.path.relpath(path2, path1)

def connect_db(db_path):
    print(f'Connecting to database: {db_path}')
    conn = sqlite3.connect(os.path.join(db_path, 'registry.db'))
    return conn

def add_to_db(file, class_name, table_name, register_name, class_type, db_path):
    # Add a new model/ds to the registry
    file = os.path.join(os.getcwd(), file)
    print(f'attempting to register {file}')
    conn = connect_db(db_path)
    c = conn.cursor()
    c.execute(f'CREATE TABLE IF NOT EXISTS {table_name} (name TEXT, url TEXT, class_name TEXT)')

    # Check if datasource already exists in registry
    ds_check = c.execute(f'SELECT * FROM {table_name} WHERE name=?', (register_name,)).fetchone()
    if ds_check is not None:
        raise ValueError(f'Datasource "{register_name}" already exists in registry.')

    repo = git.Repo(file, search_parent_directories=True)
    url = repo.remote().url
    print(f'repo url: {url}')
    commit_hash = repo.head.object.hexsha

    check_class(file, class_name, class_type)
    check_repo(repo, file, url, register_name)

    file_path = relative_path(repo.working_dir, file).replace('\\','/')
    full_url = get_github_file_url(url, file_path, commit_hash)

    c.execute(f'INSERT INTO {table_name} (name, url, class_name) VALUES (?, ?, ?)', (register_name, full_url, class_name))
    conn.commit()
    conn.close()

def read_from_db(table_name, register_name, db_path):
    # Read a model/ds from the registry
    conn = connect_db(db_path)
    c = conn.cursor()
    c.execute(f'SELECT * FROM {table_name} WHERE name=?', (register_name,))
    row = c.fetchone()
    if row is None:
        print(f'Model "{register_name}" not found in registry.')
        return
    url, class_name = row[1], row[2]
    conn.close()
    return url, class_name

def update_db(file, class_name, table_name, register_name, class_type, db_path):
    # Update an existing model/ds in the registry
    conn = connect_db(db_path)
    c = conn.cursor()
    c.execute(f'SELECT * FROM {table_name} WHERE name=?', (register_name,))
    row = c.fetchone()
    if row is None:
        print(f'Model "{register_name}" not found in registry.')
        return
    existing_url, existing_class_name = row[1], row[2]
    existing_commit = existing_url.split('/')[-6]
    existing_repo_url = existing_url.split('/blob')[0] + '.git'
    repo = git.Repo(file, search_parent_directories=True)
    url = repo.remote().url
    commit_hash = repo.head.object.hexsha

    check_class(file, class_name, class_type)
    check_repo(repo, file, url, register_name)

    file_path = relative_path(repo.working_dir, file).replace('\\','/')
    full_url = get_github_file_url(url, file_path, commit_hash)

    if (existing_class_name == class_name) and (existing_commit == commit_hash) and (existing_repo_url == url):
        print(f'Model "{register_name}" is already up to date with the latest version on the remote repository.')
        return
    
    c.execute(f'UPDATE {table_name} SET url=?, class_name=? WHERE name=?', (full_url, class_name, register_name))
    conn.commit()
    conn.close()
    print(f'Model "{register_name}" updated in registry with URL "{full_url}" and commit hash "{commit_hash}".')

def list_db(table_name, db_path):
    # List all models in the registry
    conn = connect_db(db_path)
    df = pd.read_sql(f'SELECT * FROM {table_name}',conn)
    js = df.to_dict(orient='records')
    for x in js:
        print(x)

def object_from_registry(name,table_name,db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(f'SELECT url, class_name FROM {table_name} WHERE name=?', (name,))
    row = c.fetchone()
    if row is None:
        raise ValueError(f'Error: "{name}" not found in registry under table "{table_name}"')
    url, class_name = row
    conn.close()

    base_url = '/'.join(url.split('/')[:-3])
    repo_name = base_url.split('/')[-1]
    commit_sha = url.split('/')[-2]

    # Clone the repository to a local directory
    repo_dir = f'{os.getcwd()}/{repo_name}'
    if not os.path.exists(repo_dir):
        repo = git.Repo.clone_from(base_url, repo_dir)
        print(f'Cloned repository to {repo_dir}')
    else:
        repo = git.Repo(repo_dir)
    repo.git.checkout(commit_sha)

    module_path = url.split(f'{commit_sha}/')[-1].replace('.py', '').replace('/', '.')
    module = importlib.import_module(f'{repo_name}.{module_path}')

    # Get the model class from the module
    cls = getattr(module, class_name)

    return cls


def add_run(run_id,run_config,db_path):
    # Add a new run to the registry
    conn = connect_db(db_path)
    c = conn.cursor()
    # create table with fields:
    # run_id, run_config_data, status, fit_intervals, eval_intervals, start_time, output_path
    # run_id is a unique identifier for the run
    # run_config_data is the json data for the run config
    # status is the status of the run (running, completed, failed)
    # start_time is the start time of the run
    # output_path is the path to the output of the run
    c.execute(f'CREATE TABLE IF NOT EXISTS runs (run_id TEXT, run_config_data TEXT, status TEXT, start_time TEXT, output_path TEXT)')
    # Check if run already exists in registry
    run_id = run_config['run_id']
    run_check = c.execute(f'SELECT * FROM runs WHERE run_id=?', (run_id,)).fetchone()
    if run_check is not None:
        raise ValueError(f'Run "{run_id}" already exists in registry.')
    # Insert run into registry
    run_config_data = json.dumps(run_config)
    now = datetime.now()
    start_time = now.strftime("%d/%m/%Y %H:%M:%S")
    c.execute(f'INSERT INTO runs (run_id, run_config_data, status, start_time) VALUES (?, ?, ?, ?)', (run_id, run_config_data, 'running', start_time))
    conn.commit()
    conn.close()
