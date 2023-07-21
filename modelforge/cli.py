import json
import os

import click

from pathlib import Path
from dask.distributed import Client

from modelforge.core.components import MFModel,MFDatasource
from modelforge.core.runner import MFRunner
from modelforge.utils.registry import add_run, add_to_db, read_from_db, update_db, list_db

@click.group()
def modelforge():
    print(f'Welcome to modelforge! We read from {CONFIG_PATH}')
    pass

###################################### DATABASE ######################################
@modelforge.group()
def database():
    pass

@database.command()
@click.option('--path', '-p', required=True, type=click.Path(exists=False), help='Path to the directory containing the databases')
def set(path):
    print(path)
    Path(path).mkdir(parents=True, exist_ok=True)
    with open(CONFIG_PATH) as f:
        config = json.load(f)
    config['database_path'] = path
    with open(CONFIG_PATH, 'w') as f:
        json.dump(config, f)
    print(f'Database path set to "{path}".')

@database.command()
def get():
    print(f'Registry path: {DATABASE_PATH}')


###################################### MODEL ######################################
@modelforge.group()
def model():
    pass

@model.command()
@click.option('--name', '-n', required=True, help='Universal name of the model')
@click.option('--file', '-f', required=True, type=click.Path(exists=True), help='Path to Python module containing model class')
@click.option('--class-name', '-c', required=True, help='Name of the model class in the Python module')
def add(file, class_name, name):
    add_to_db(file, class_name, 'models', name, MFModel, DATABASE_PATH)

@model.command()
@click.option('--name', '-n', required=True, help='Universal name of the model')
@click.option('--file', '-f', required=True, type=click.Path(exists=True), help='Path to Python module containing model class')
@click.option('--class-name', '-c', required=True, help='Name of the model class in the Python module')
def update(name, file, class_name):
    update_db(file, class_name, 'models', name, MFModel, DATABASE_PATH)

###################################### DATASOURCE ######################################
@modelforge.group()
def datasource():
    pass

@datasource.command()
@click.option('--file', '-f', required=True, type=click.Path(exists=True), help='Path to Python module containing datasource class')
@click.option('--class-name', '-c', required=True, help='Name of the datasource class in the Python module')
@click.option('--name', '-n', required=True, help='Universal name of the datasource')
def add(file, class_name, name):
    add_to_db(file, class_name, 'datasources', name, MFDatasource, DATABASE_PATH)

@datasource.command()
@click.option('--name', '-n', required=True, help='Universal name of the datasource')
@click.option('--file', '-f', required=True, type=click.Path(exists=True), help='Path to Python module containing datasource class')
@click.option('--class-name', '-c', required=True, help='Name of the datasource class in the Python module')
def update(name, file, class_name):
    update_db(file, class_name, 'datasources', name, MFDatasource, DATABASE_PATH)

@datasource.command()
def list():
    list_db('datasources', DATABASE_PATH)

###################################### RUNS ######################################
@modelforge.group()
def run():
    pass

@run.command()
@click.option('--run_name', '-m', required=True, help='Name of the to run')
@click.option('--model', '-m', required=True, help='Name of the registered model to run')
@click.option('--datasource', '-p', required=True, help='Name of the registered datasource to use as input')
@click.option('--run_config', '-c', type=click.Path(exists=True), required=True, help='Path to run configuration file')
def run(run_name,model, datasource, run_config):
    run_config_data = dict()
    # Load the configuration file
    with open(run_config, 'r') as f:
        run_config_data['user_config'] = json.load(f)

    run_config_data['database_path'] = DATABASE_PATH
    run_config_data['model'] = model
    run_config_data['datasource'] = datasource

    run_config_data['datasource_url'],run_config_data['datasource_class_name'] = read_from_db(datasource, 'datasources', DATABASE_PATH)
    run_config_data['model_url'],run_config_data['model_class_name'] = read_from_db(model, 'models', DATABASE_PATH)

    # Run the model on the datasource using the MFRunner class
    runner = MFRunner(run_config_data['user_config'])
    add_run(run_name, run_config_data, DATABASE_PATH)

    client = Client(config['dask_scheduler_address'])
    client.submit(runner.train) # does not block

###################################### CONFIG ######################################
BASE_PATH = os.path.join(os.path.expanduser('~'), '.modelforge')
CONFIG_PATH = os.path.join(BASE_PATH,'config.json')
DATABASE_PATH = os.path.join(BASE_PATH,'databases')
default_config = {'database_path': DATABASE_PATH,'dask_scheduler_address':None,'output_path':os.path.join(BASE_PATH,'output')}
print(f'Config path: {CONFIG_PATH}')
if os.path.isfile(CONFIG_PATH):
    with open(CONFIG_PATH) as f:
        config = json.load(f)
    DATABASE_PATH = config['database_path']
    for k,v in default_config.items():
        if k not in config:
            config[k] = v
else:
    print(f'creating base path: {BASE_PATH}')
    Path(BASE_PATH).mkdir(parents=True, exist_ok=True)
    print(f'creating database path: {CONFIG_PATH}')
    with open(CONFIG_PATH, 'w') as f:
        json.dump(default_config, f)
    print(f'creating config file: {CONFIG_PATH}')
    print(f'default config: {default_config}')
Path(DATABASE_PATH).mkdir(parents=True, exist_ok=True)
modelforge()