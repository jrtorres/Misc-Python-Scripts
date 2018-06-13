import os
import json
import sys
import pysolr
import logging
import argparse
import time

ROWS_BATCH_AMOUNT = 200
SPLI_DOC_AMOUNT = 5000
DEBUG_INTERVAL = 1000
ALL_DOCS_QUERY = "*:*"
OUTPUT_TSTAMP = time.strftime("%Y%m%d-%H%M%S")
     
def initialize_logger(log_level, name):
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    if not logger.handlers:
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(log_level)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s : %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    return logger

def parse_creds_file(credentials_file_path, key_name):
    ''' 
    Parses the R&R service credentials from a json file. Expects them in the following format:
    {  
        "rnr_instance_1": {
            "url": https://gateway.watsonplatform.net/retrieve-and-rank/api",
            "username": "username",
            "password": "password"
        },
        "rnr_instance_2": {
            "url": "https://gateway.watsonplatform.net/retrieve-and-rank/api",
            "username": "username",
            "password": "password"
        }
    }
    '''
    with open(credentials_file_path) as user_creds_file:
        try:
            all_creds = json.load(user_creds_file)
            credentials = all_creds[key_name]
            LOGGER.debug('parse_creds_file  -  Credentials file loaded [%s]' % json.dumps(credentials))
            return credentials['url'], credentials['username'], credentials['password']
        except KeyError as ke:
            LOGGER.error('parse_creds_file  -  Expected keys do not exist: %s' % ke.message)
            raise ke
        except ValueError as ex:
            LOGGER.error('parse_creds_file  -  Credentials file not valid: %s' % ex.message)
            raise ValueError('Unable to parse credentials file: %s' % ex.message)

def get_solr_client(credentials_file_path, credentials_key_name, rnr_clusterid, rnr_collection):
    '''
    Instantiates a pysolr client to use against the R&R instance specified in the credentials file. 
    '''
    rnr_url, rnr_uname, rnr_pwd = parse_creds_file(credentials_file_path, credentials_key_name)
    base_url = rnr_url.replace('https://',
                                'https://' + rnr_uname + ':' +
                                rnr_pwd + '@')
    url = base_url + '/v1/solr_clusters/{0}/solr/{1}'.format(
            rnr_clusterid, rnr_collection)
    solr_client = pysolr.Solr(url)
    
    LOGGER.debug("get_solr_client - Pysolr client created to url: %s" % url)
    assert solr_client is not None
    return solr_client

def get_documents(solr_client, filter_param):
    '''
    Queries documents from Solr using *:* query (and optional filter parameter). Assumes there is an
    id field in the document that is used to sort. Uses cursor paging to gathers documents in batches, 
    yielding them to be saved to the filesystem.
    '''
    kwargs = {
        'sort': 'id asc', 
        'rows': ROWS_BATCH_AMOUNT,
    }
    if filter_param is not None:
        kwargs['fq'] = filter_param 

    res = None
    doc_count = 0
    while True:
        if res == None:
            res = solr_client.search(ALL_DOCS_QUERY, cursorMark="*", **kwargs)
        else:
            res = solr_client.search(ALL_DOCS_QUERY, cursorMark=res.nextCursorMark, **kwargs)

        if not res.docs:
            break

        for doc in res.docs:
            doc_count += 1
            if doc_count % DEBUG_INTERVAL == 0:
                LOGGER.debug('get_documents - Number of documents retrieved = %d' % doc_count)
            yield doc

def get_document_count(client, filter_param):
    kwargs = {
        'rows': 0,
    }
    if filter_param is not None:
        kwargs['fq'] = filter_param 

    results = client.search(ALL_DOCS_QUERY, **kwargs)
    return results.hits

def write_documents(json_docs, file_name):
    LOGGER.debug("write_documents - Writing output file %s with %d objects." % (file_name, len(json_docs)))
    with open(file_name, "w") as output_file:
        json.dump(json_docs, output_file, indent=4)

    ###### Larger files might have to switch to send docs one at a time into file, separated by line. Or switch to ijson or pandas
    #with open(TRAINING_DATA_FILENAME, "w") as output_file:
    #    for doc in get_documents(pysolr_client):
    #        print("Adding document id %s" % doc["id"])
    #        output_file.write("{}\n".format(json.dumps(doc)))
    #        #json.dump(doc, output_file, sort_keys=True, indent=4))
    ######
    #data = []
    #with open('file') as f:
    #for line in f:
    #    data.append(json.loads(line))

def backup_driver(parameters):
    '''
    Uses a pysolr client to pull documents from Solr and store them to the filesystem (as json files). Documents are stored
    either in batches (if split parameter used) or as a single file (json array). Documents pulled can be filtered by providing 
    a filter parameter.
    '''
    LOGGER.info("backup_driver - Starting Solr backup, using cluser [%s] and collection [%s]. Saving documents to [%s]" % (parameters.cluster_id, parameters.collection_name, parameters.output_dir))
    LOGGER.debug("backup_driver - Filter set to [%s], Split files is [%s]" % (parameters.filter_param, parameters.split_files))

    if not os.path.isdir(parameters.output_dir):
        LOGGER.error("backup_driver - Specified output is not a directory." )
        raise ValueError("Could not find the output directory.")

    pysolr_client = get_solr_client(parameters.user_creds_file, parameters.user_creds_key, parameters.cluster_id, parameters.collection_name)
    
    LOGGER.info("backup_driver - Total number of documents in index: %d" % get_document_count(pysolr_client, None))
    if parameters.filter_param is not None:
        LOGGER.info("backup_driver - Total number of documents in filtered request: %d" % get_document_count(pysolr_client, parameters.filter_param))
    
    #Quick version but slow, probably going to be an issue for large index
    #Larger index might pull all doc_ids and thread multiple pulls or based on fq list
    all_data = []
    counter = 0
    for doc in get_documents(pysolr_client, parameters.filter_param):
        all_data.append(doc)
        if(parameters.split_files):
            if(len(all_data) % SPLI_DOC_AMOUNT == 0):
                write_documents(all_data, "%s/%s_%s_%d.json" % (parameters.output_dir, parameters.collection_name, OUTPUT_TSTAMP, counter + 1))
                all_data = []
                counter += 1
            
    if(len(all_data) > 0):
        write_documents(all_data, "%s/%s_%s_%d.json" % (parameters.output_dir, parameters.collection_name, OUTPUT_TSTAMP, counter + 1))

if __name__ == '__main__':
    if sys.version_info[0] < 3:
        raise Exception("Python 3 or higher version is required for this script.")

    parser = argparse.ArgumentParser(prog="python %s)" % os.path.basename(__file__), description='Script to pull documents from a Solr index')
    parser.add_argument('-creds-file', dest='user_creds_file', required=True, help='Credentials file name')
    parser.add_argument('-creds-key', dest='user_creds_key', required=True, help='Credentials file key')
    parser.add_argument('-output-location', dest='output_dir', required=True, help='Directory to store output from Solr')
    parser.add_argument('-cid', dest='cluster_id', required=True, help='Cluster ID.')
    parser.add_argument('-cname', dest='collection_name', required=True, help='Collection name.')

    parser.add_argument('-s', dest="split_files", action='store_true', help='Split output into multiple files.')
    parser.add_argument('-f', dest='filter_param', help='Filter value to pass to fq parameter (quote multispace)')
    parser.add_argument('-d', dest="log_level", help="Set DEBUG logging level", action="store_const",
                        const=logging.DEBUG, default=logging.INFO)

    args = parser.parse_args()
    LOGGER = initialize_logger(args.log_level, os.path.basename(__file__))
    backup_driver(args)
