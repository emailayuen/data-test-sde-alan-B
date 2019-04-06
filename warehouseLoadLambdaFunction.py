"""
This code is for Task B - Deployment
"""
import os
import re
from multiprocessing import Pool
from google.cloud import bigquery
from pathlib import Path

#constants
SQL_SCRIPTS_ROOT_FOLDER = 'res'
SQL_SCRIPTS_DIR = ['tmp', 'final']
SQL_SCRIPTS_EXT = 'sql'
RAW_SCRIPTS_FOLDER = 'db_init'
PROJECT_ID = 'data-test-sde'
LOCATION = 'US'
KEY_FILE_PATH = 'key/data-test-sde-f4c4c4a2b7da.json'
DATASET_RAW = 'raw'
DATASET_TMP = 'tmp'
DATASET_FINAL = 'final'

def extrapolate_tables(content, schemas):
    '''
    function that finds dependent tables from sql statement
    assumes tables are identified by surrounding "`" character in sql
    '''
    tables = re.findall(r"`(.*?)`", content, re.DOTALL)
    dependent_table=[]
    for table in tables:
        schema = table.split('.')[0]
        if schema in schemas:
            dependent_table.append(table)
    return dependent_table

def build_master_table(schemas):
    '''
    return list of tables and respective dependencies
    '''
    master={}
    working_dir = Path.cwd()
    for ds in SQL_SCRIPTS_DIR:
        for file in working_dir.glob('{0}/{1}/*.*'.format(SQL_SCRIPTS_ROOT_FOLDER, ds)):
            table='{0}.{1}'.format(ds, file.stem)
            with open(file) as f:
                content = f.readlines()
            sql_content = ''.join(content)
            master[table] = extrapolate_tables(sql_content, schemas)
    return master

class Node:
    '''
    class object representing a graph data structure (nodes and edges)
    in this instance, an edge from one node to another represents dependency
    '''
    def __init__(self, name, level):
        self.name = name
        self.level = level
        self.edges = []
    
    def addEdge(self, node):
        self.edges.append(node)

def resolve_dependency(node, resolved):
    '''
    recursive algorithm that traverses through each node and their dependencies
    returns synchronous run order
    '''
    for edge in node.edges:
        if edge not in resolved:
            resolve_dependency(edge, resolved)
    if node not in resolved:
        resolved.append(node)

def build_master_table_ordered_sync(master_table):
    '''
    return list of tables, respective dependencies and run sequence
    '''
    # create nodes
    table_node_dict = {}
    for table_name in master_table:
        table_node_dict[table_name] = Node(table_name, None)
    # add edges to nodes
    for table_name in master_table:
        node = table_node_dict.get(table_name)
        edges = master_table.get(table_name, None)
        if edges:
            for edge_str in edges:
                node.addEdge(table_node_dict.get(edge_str))
    # run algorithm to get sequence
    table_node_ordered = []
    for table_node in table_node_dict:
        resolve_dependency(table_node_dict[table_node], table_node_ordered) 
    return table_node_ordered

def get_process(node):
    node_name_split = node.name.split('.')
    return '{0}/{1}/{2}.{3}'.format(SQL_SCRIPTS_ROOT_FOLDER, node_name_split[0], node_name_split[1], SQL_SCRIPTS_EXT)

def build_master_table_levels(master_table_graph):
    '''
    algorithm that assigns a "level" to a node based on dependencies
    '''
    current_level = 1
    #1. set level for root tables (tables without any dependencies)
    for node in master_table_graph:
        if len(node.edges) == 0:
            node.level = current_level
    #2. set level for tables with dependencies
    while len([node.name for node in master_table_graph if node.level is None]) > 0:
        processed_level_tables = [level_node.name for level_node in master_table_graph if (level_node.level is not None and level_node.level <= current_level)]
        for node in master_table_graph:
            if node.level is None:
                node_edges = [e.name for e in node.edges]
                complete_match =  all(elem in processed_level_tables for elem in node_edges)
                if complete_match:
                    node.level = current_level + 1
        current_level += 1
    return master_table_graph

def get_max_parallel_run(master_list):
    '''
    derive max number of processes that will run at any one time
    '''
    jobs = []
    [jobs.append(node.level) for node in master_list]
    freq = {} 
    for items in jobs: 
        freq[items] = jobs.count(items)
    max_key = max(freq, key=freq.get)
    return freq[max_key]

def get_biquery_client():
    '''
    return client connection to bigquery
    '''
    client = bigquery.Client.from_service_account_json(KEY_FILE_PATH, project=PROJECT_ID)
    return client

def execute_biquery_script(script_path):
    '''
    wrapper to execute bigquery script
    '''
    client = get_biquery_client()
    content = []
    with open(script_path) as f:
        content = f.readlines()
    query = ''.join(content)
    query_job = client.query(query)  # API request
    query_job.result() # waits for job to complete
    print('Script "{0}" ran successfully...'.format(script_path))

def execute_big_query_table_load(script_path):
    '''
    wrapper to execute bigquery script and load into destination table
    '''
    script_path_split = script_path.split('/')
    ds_id = script_path_split[len(script_path_split)-2]
    tbl_script = script_path_split[len(script_path_split)-1]
    tbl_id = tbl_script.split('.')[0]
    client = get_biquery_client()
    job_config = bigquery.QueryJobConfig()
    #set the destination table
    tbl_ref = client.dataset(ds_id).table(tbl_id)
    job_config.destination = tbl_ref
    content = []
    with open('{0}/{1}/{2}'.format(SQL_SCRIPTS_ROOT_FOLDER, ds_id, tbl_script)) as f:
        content = f.readlines()
    query = ''.join(content)
    # start the query, passing in extra config
    query_job = client.query(
        query,
        location=LOCATION,
        job_config = job_config
    ) # API request - starts the query
    query_job.result() # waits for the query to finish
    print('Query results loaded to table {}'.format(tbl_ref.path))

def initialise_dataset(ds_name):
    '''
    function that creates a new biquery dataset
    will drop and re-create if dataset already exists
    '''
    client = get_biquery_client()
    ds_ref = client.dataset(ds_name)
    try:
        # get dataset if exists
        client.get_dataset(ds_ref)
        dataset = bigquery.Dataset(ds_ref)
        # drop all tables in dataset
        table_list_items = list(client.list_tables(dataset))
        for table_item in table_list_items:
            client.delete_table(table_item.reference)
        # drop dataset
        client.delete_dataset(ds_ref)
    except:
        # if creating for the first time
        pass
    #re-create dataset
    ds = bigquery.Dataset(ds_ref)
    dataset = client.create_dataset(ds)
    print('Dataset "{0}" created...'.format(ds.dataset_id))

def seed_raw_tables():
    '''
    function that retrieves and executes seed scripts for raw tables
    '''
    for root, dirs, files in os.walk(RAW_SCRIPTS_FOLDER):
        for filename in files:
            #raw_scripts.append(filename)
            script_path = '{0}/{1}'.format(RAW_SCRIPTS_FOLDER, filename)
            execute_biquery_script(script_path)

def set_response(code, body):
    '''
    helper function to create response object
    '''
    resp = {}
    resp['statusCode']=code
    resp['body']=body
    return resp

def lambda_handler(event, context):
    '''
        ##############################################################################################
        B.1
        Approach:
            1. Utilise same code in A.3 to fetch master table of dependencies and levels
            2. Rather than run fake scripts, simply execute real sql scripts against bigquery
        
        ##############################################################################################
    '''
    try:
        # 1. generate master table of dependencies
        master_table = build_master_table(schemas=['tmp', 'final'])
        master_table_graph_sync = build_master_table_ordered_sync(master_table)
        master_table_graph_levels = build_master_table_levels(master_table_graph_sync)
        # 2. initialise big query datasets
        #initialise_dataset(DATASET_RAW) -- UNCOMMENT TO INCLUDE RAW
        initialise_dataset(DATASET_TMP)
        initialise_dataset(DATASET_FINAL)
        # seed raw tables
        #seed_raw_tables() -- UNCOMMENT TO INCLUDE RAW
        # 3. run master job
        processor_count = get_max_parallel_run(master_table_graph_levels)
        pool = Pool(processes=processor_count) # multi processing
        max_level = master_table_graph_levels[len(master_table_graph_levels)-1].level
        current_level = 1
        while current_level <= max_level:
            current_level_processes = []
            for node in master_table_graph_levels:
                if node.level == current_level:
                    current_level_processes.append(get_process(node))
            #execute scripts for the level
            print('\nBegin processing level {0} nodes...'.format(str(current_level)))
            pool.map(execute_big_query_table_load, current_level_processes)
            current_level += 1
        response = set_response(200, 'Warehouse loaded successfully')
    except Exception as ex:
        response = set_response(500, ex)
    return response

if __name__ == '__main__':
  event = {
  }
  lambda_handler(event, None)