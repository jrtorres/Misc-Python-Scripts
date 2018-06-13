import sys
import os
import requests
import json
import logging
import shlex
import subprocess
import time
import argparse
import ntpath
import threading

from concurrent.futures import ThreadPoolExecutor, wait, as_completed
from requests.structures import CaseInsensitiveDict
from collections import defaultdict

WDS_API_VERSION = '2018-03-05'
WDS_SUPPORTED_FILE_TYPES = ('.json', '.pdf','.html', '.doc', '.docx')
JSON_FILE_TYPES = ('.json')
DOC_UPLOAD_DEFAULT_STATUS = "Unknown"
MAX_NUMBER_THREADS = 2#8
OUTPUT_INTERVAL = 1000
THREAD_SLEEP_TIME = 0.02
RETRY_SLEEP_TIME = 0.03
RETRY_FILE_NAMES = 'discovery_retry.log'
INGEST_FILE_EXTENSION = '_discovery_ingestion.log'

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
    Parses the Discovery service credentials from a json file. Expects them in the following format:
    {  
        "discovery_instance_1": {
            "url": "https://gateway.watsonplatform.net/discovery/api",
            "username": "username",
            "password": "password"
        },
        "discovery_instance_2": {
            "url": "https://gateway.watsonplatform.net/discovery/api",
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

def write_json_to_file(output_json_content, output_file_name):
    with open(output_file_name, 'w', encoding='utf8') as output_file:
        json.dump(output_json_content, output_file, indent=4)

def upload_file(disco_instance, document_id, file_tuple):
    ''' 
    Attempts to upload a file tuple to Discovery using REST API (tuple includes the file name, the file content, and file content type). 
    Has a single retry on HTTP code 429 status. Returns a response with following format"
    {
        "success": True / False
        "response_code": HTTP response code
        "doc_id" : "The uploaded document Id, or None if error/exception"
        "doc_state" : "The document upload state or Unknown if error/exception"
    }, original_file_tuple_input
    '''
    d_status = DOC_UPLOAD_DEFAULT_STATUS
    if document_id is not None:
        discovery_url = disco_instance.url + '/v1/environments/{0}/collections/{1}/documents/{2}?version={3}'.format(disco_instance.env_id, disco_instance.col_id, document_id, WDS_API_VERSION)
    else: 
        discovery_url = disco_instance.url + '/v1/environments/{0}/collections/{1}/documents?version={2}'.format(disco_instance.env_id, disco_instance.col_id, WDS_API_VERSION)
    auth = (disco_instance.uname, disco_instance.pwd)
    headers = CaseInsensitiveDict()
    headers['accept'] = 'application/json'

    try:
        LOGGER.debug('upload_file  -  [%s] Upload request started. Upload URL [%s] || File metadata filename: [%s] || mime_type: [%s]' % (threading.current_thread().name, discovery_url, file_tuple[0], file_tuple[2]))
        response = requests.request('POST', url=discovery_url, auth=auth, headers=headers, files={'file': file_tuple})
        LOGGER.debug('upload_file  -  [%s] Upload response code: [%d]. Content: [%s]' % (threading.current_thread().name, response.status_code, response.text.replace("\r\n", "").replace("\n","")))

        if 200 <= response.status_code <= 299:
            response_json = response.json()
            if 'status' in response_json and response_json['status']  == 'ERROR':
                return {'success': False, 'response_code':str(response.status_code), 'doc_id': document_id, 'doc_state': d_status }, file_tuple
            return {'success': True, 'response_code':str(response.status_code), 'doc_id': response_json['document_id'], 'doc_state': response_json['status'] }, None
        elif response.status_code == 429:
            time.sleep(RETRY_SLEEP_TIME)
            LOGGER.debug('upload_file  -  [%s] Retry Upload request started. Upload URL [%s] || File metadata filename: [%s] || mime_type: [%s]' % (threading.current_thread().name, discovery_url, file_tuple[0], file_tuple[2]))
            response = requests.request('POST', url=discovery_url, auth=auth, headers=headers, files={'file': file_tuple})
            LOGGER.debug('upload_file  -  [%s] Retry Upload response code: [%d]. Content: [%s]' % (threading.current_thread().name, response.status_code, response.text.replace("\r\n", "").replace("\n","")))
            if 200 <= response.status_code <= 299:
                response_json = response.json()
                if 'status' in response_json and response_json['status']  == 'ERROR':
                    return {'success': False, 'response_code':str(response.status_code), 'doc_id': document_id, 'doc_state': d_status }, file_tuple
                return {'success': True, 'response_code':str(response.status_code), 'doc_id': response_json['document_id'], 'doc_state': response_json['status'] }, None
            else:
                return {'success': False, 'response_code':str(response.status_code), 'doc_id': document_id, 'doc_state': d_status }, file_tuple
        else:
            return {'success': False, 'response_code':str(response.status_code), 'doc_id': document_id, 'doc_state': d_status }, file_tuple
    except Exception as e:
        msg = 'Exception occured'
        if hasattr(e, 'reason'):
            msg = msg + '. Reason: ' + e.reason
        elif hasattr(e, 'code'): 
            msg = msg + '. Code' + e.code
        elif hasattr(e, 'message'): 
            msg = msg + '. Message' + e.message
        
        LOGGER.error('upload_file  -  [%s] Upload failure: %s' % (threading.current_thread().name, msg) )
        return {'success': False, 'response_code': 'Unknown', 'doc_id': document_id, 'doc_state': d_status }, file_tuple

def process_json_array(disco_instance, json_file_path, json_data):
    '''
    Special case, handles processing of a file with JSON array content which is iterated and uploaded to Discovery (each object in the array becomes a document
    in Discovery). Uses threads to upload objects in the JSON array with predefined sleep between thread requests to minimize throttling 
    (429s). Returns a response with following format:
    {
        "documents_processed_count": # of documents in json array
        "documents_successful_upload_count" : # of documents that successfully uploaded
        "documents_failed_upload_count" : # of documents that failed to upload (and can be retried with stored input data)
    }, response_code_stats, doc_id status array, failed_documents_array
    '''
    LOGGER.info('process_json_array  -  JSON file processing started. Number of JSON documents to ingest [%d]' % len(json_data))
    process_jsonarray_stats = defaultdict(int)
    response_code_stats = defaultdict(int)
    failed_docs = []
    doc_results = []
    with ThreadPoolExecutor(max_workers=MAX_NUMBER_THREADS) as executor:
        upload_tasks = []
        for doc in json_data:
            process_jsonarray_stats['documents_processed_count'] += 1
            document_id = doc['id'] if 'id' in doc else None
            file_name = (doc['id'] + '.json') if 'id' in doc else (os.path.splitext(ntpath.basename(json_file_path))[0] + '_' + str(process_jsonarray_stats['documents_processed_count']) + '.json')
            
            doc.pop("id", None) #Remove id from document so that no notices are generated.
            file_tuple = (file_name, json.dumps(doc), 'application/json')
            upload_task = executor.submit(upload_file, disco_instance, document_id, file_tuple)
            upload_tasks.append(upload_task)

            time.sleep(THREAD_SLEEP_TIME)
            if process_jsonarray_stats['documents_processed_count'] % OUTPUT_INTERVAL == 0:
                LOGGER.info('process_json_array  -  JSON documents upload submitted. Current status: [%s]' % json.dumps(process_jsonarray_stats))

        for upload_task in as_completed(upload_tasks):
            upload_res, input_data = upload_task.result()
            response_code_stats[upload_res['response_code']] += 1
            #doc_results.append(upload_res["doc_id"] + " | " + str(upload_res["success"]) + " | " + upload_res["doc_state"] )
            if upload_res['success']:
                doc_results.append(upload_res['doc_id'] + ' | ' + str(upload_res['success']) + ' | ' + upload_res['doc_state'] )
                process_jsonarray_stats['documents_successful_upload_count'] += 1
            else:
                if input_data is not None:
                    # Reinject document_id for failed documents. Only do this for json array since I'm writing
                    # just the failed docs to a new file, instead of retrying the entire file with all json docs.
                    if upload_res['doc_id'] is not None:
                        doc_results.append(upload_res['doc_id'] + ' | ' + str(upload_res['success']) + ' | ' + upload_res['doc_state'] )
                        f_doc = json.loads(input_data[1])
                        f_doc['id'] = upload_res['doc_id']
                        failed_docs.append(f_doc)
                    else:
                        doc_results.append(input_data[0] + ' | ' + str(upload_res['success']) + ' | ' + upload_res['doc_state'] )
                        failed_docs.append(json.loads(input_data[1]))
                else:
                    LOGGER.error('process_json_array  -  Document upload failed but no input captured for retry. Original file name [%s]' % json_file_path)
    
    process_jsonarray_stats['documents_failed_upload_count'] = len(failed_docs)
    LOGGER.info('process_json_array  -  Finished processing JSON file: Statistics [%s]' % json.dumps(process_jsonarray_stats))
    return process_jsonarray_stats, response_code_stats, doc_results, failed_docs

def process_file(disco_instance, input_file, output_directory):
    '''
    Handles processing of a file to upload into discovery. Writes results of uploading each file into log file.
    Returns the number of files that were attempted to upload, the number of files that were successfully uploaded, and
    the name of file that has any documents that need to be retried.
    '''
    LOGGER.info('process_file  -  Single file processing started - [%s]' % input_file)
    file_process_start_time = time.time()
    retry_file_name = None 
    valid_doc_upload_attempt_count = 0
    valid_doc_upload_success_count = 0
    response_code_stats = defaultdict(int)
    doc_ingest_results = []
    if not input_file.lower().endswith(WDS_SUPPORTED_FILE_TYPES) or not os.path.isfile(input_file):
        LOGGER.debug('process_file  -  Provided file is either not a file or not a supported file type')
        return valid_doc_upload_attempt_count, valid_doc_upload_success_count, retry_file_name

    with open(input_file, 'r',  encoding='utf8') as current_file:
        if hasattr(current_file, 'name'):
            file_name = current_file.name
            if not file_name:
                file_name = ntpath.basename(input_file)

        #Special JSON handling
        if input_file.lower().endswith(JSON_FILE_TYPES):
            json_data = json.load(current_file)
            if json_data is  None:  
                LOGGER.warning('process_file - JSON File with no data - [%s]' % file_name)
            elif isinstance(json_data, list):
                array_process_stats, response_code_stats, doc_ingest_results, failed_docs = process_json_array(disco_instance, input_file, json_data)
                valid_doc_upload_attempt_count = array_process_stats['documents_processed_count']
                valid_doc_upload_success_count = array_process_stats['documents_successful_upload_count']
                if failed_docs:
                    retry_file_name = os.path.join(output_directory, os.path.splitext(ntpath.basename(input_file))[0] + '_failed.json')
                    write_json_to_file(failed_docs, retry_file_name )
                    LOGGER.warning('process_file - Writing failed json documents to new file %s with %d documents.' % (retry_file_name, len(failed_docs)))
                    LOGGER.warning('process_file - Response code counts: [%s]' % json.dumps(response_code_stats))
            elif isinstance(json_data, dict):
                valid_doc_upload_attempt_count = 1
                document_id = json_data['id'] if 'id' in json_data else None

                json_data.pop('id', None) #Remove id from document to avoid warning notice.
                file_tuple = (file_name, json.dumps(json_data), 'application/json')
                upload_res, input_data = upload_file(disco_instance, document_id, file_tuple)
                response_code_stats[upload_res['response_code']] += 1
                doc_ingest_results.append(file_name + ' [' + upload_res['doc_id'] + ']  | ' + str(upload_res['success']) + ' | ' + upload_res['doc_state'])
                if upload_res['success']:
                    valid_doc_upload_success_count = 1
                else:
                    retry_file_name = input_file
            else:
                LOGGER.warning('process_file  -  Not a know JSON type')
        else:
            valid_doc_upload_attempt_count = 1
            file_tuple = (file_name, current_file, 'application/octet-stream')
            upload_res = upload_file(disco_instance, None, file_tuple)
            response_code_stats[upload_res['response_code']] += 1
            doc_ingest_results.append(file_name + ' [' + upload_res['doc_id'] + ']  | ' + str(upload_res['success']) + ' | ' + upload_res['doc_state'])
            if upload_res['success']:
                valid_doc_upload_success_count = 1
            else:
                retry_file_name = input_file

    file_process_elapsed_time = time.time() - file_process_start_time                  
    LOGGER.info('process_file  -  Single file [%s] processing completed in [%f] seconds' % (input_file, file_process_elapsed_time))

    log_file_name = os.path.join(output_directory, os.path.splitext(ntpath.basename(input_file))[0] + INGEST_FILE_EXTENSION)
    LOGGER.info('process_file  -  Writting ingestion results to log file [%s]' % log_file_name)
    with open(log_file_name,'w', encoding='utf8') as o:
        o.write('\n'.join(doc_ingest_results))

    return valid_doc_upload_attempt_count, valid_doc_upload_success_count, retry_file_name

def process_fs_input(disco_instance, input_path, output_directory):
    '''
    Attempts to process the input provided and upload it to the given discovery instance. The input can either be
    a file or a directory with files in it. Writes the names of files that need to be retried to a log file.
    '''
    LOGGER.info('process_fs_input  -  Input file location processing started - [%s]' % input_path)
    failed_file_names = []
    process_dir_stats = defaultdict(int)
    if os.path.isdir(input_path):
        LOGGER.debug("process_fs_input  -  Input file location is a directory. Number of files to process: [%d]" % len(os.listdir(input_path)))
        for subdir, dirs, files in os.walk(input_path):
            for single_file in files:
                process_dir_stats['files_processed_count'] += 1
                upload_attempted_counter, upload_completed_counter, failed_fname = process_file(disco_instance, os.path.join(subdir, single_file), output_directory)
                process_dir_stats['documents_successful_upload_cout'] += upload_completed_counter
                if failed_fname:
                    failed_file_names.append(failed_fname)
                    
    elif os.path.isfile(input_path):
        LOGGER.debug('process_fs_input  -  Input file location is a file.')
        process_dir_stats['files_processed_count'] = 1
        upload_attempted_counter, upload_completed_counter, failed_fname = process_file(disco_instance, input_path, output_directory)
        process_dir_stats['documents_processed_count'] = upload_attempted_counter
        process_dir_stats['documents_successful_upload_cout'] += upload_completed_counter
        if failed_fname:
            failed_file_names.append(failed_fname)
    else:
        LOGGER.warning('process_fs_input  -  Provided input file location is not a file nor a directory [ignoring]')

    if failed_file_names:
        out_file_name = os.path.join(output_directory,RETRY_FILE_NAMES)
        LOGGER.info('process_fs_input  -  Failed document names written to log file [%s]' % out_file_name)
        with open(out_file_name,'w', encoding='utf8') as o:
            o.write('\n'.join(failed_file_names))

    LOGGER.info('process_fs_input  -  Finished processing file system location, statistics [%s]' % json.dumps(process_dir_stats, sort_keys=True, indent=4))

def upload_driver(parameters):
    LOGGER.info("main  -  Input file/directory: [%s]" % (parameters.input_seed))

    wds_url, wds_uname, wds_pwd = parse_creds_file(parameters.user_creds_file, parameters.user_creds_key) 
    input_location = parameters.input_seed
    output_location = parameters.output_dir

    out_sub_dir = 'DiscoveryUpload_Output_' + time.strftime("%Y%m%d_%H%M%S")
    final_output_dir = os.path.join(output_location, out_sub_dir)
    try:
        if not os.path.exists(final_output_dir):
            os.makedirs(final_output_dir)
    except OSError:
        LOGGER.error("main  -  Error creating output directory: [%s] " %  final_output_dir)
        raise

    wds_instance = DiscoveryInstance(url=wds_url, uname=wds_uname, pwd=wds_pwd, env_id=parameters.environment_id,
                                col_id=parameters.collection_id)

    #VALIDATE DISCOVERY INFO
    #TODO - valid environment & collection (exists, has space, etc...)

    if input_location.endswith("/"):
        input_location = input_location[0:len(input_location) - 1]
    
    process_fs_input(wds_instance, input_location, final_output_dir)

class DiscoveryInstance:
    def __init__(self, url, uname, pwd, env_id=None, col_id=None):
        self.url = url
        self.uname = uname
        self.pwd = pwd
        self.env_id = env_id
        self.col_id = col_id

if __name__ == '__main__':
    if sys.version_info[0] < 3:
        raise Exception("Python 3 or higher version is required for this script.")

    parser = argparse.ArgumentParser(prog="python %s)" % os.path.basename(__file__), description='Script that uploads documents to a WDS collection')
    parser.add_argument('-creds-file', dest='user_creds_file', required=True, help='WDS credentials file name')
    parser.add_argument('-creds-key', dest='user_creds_key', required=True, help='WDS credentials key')
    parser.add_argument('-input-location', dest='input_seed', required=True, help='File or Directory of documents being ingested.')
    parser.add_argument('-output-location', dest='output_dir', required=True, help='Directory to store output and failed documents')
    parser.add_argument('-environment', dest='environment_id', required=True, help='WDS environment ID')
    parser.add_argument('-collection', dest='collection_id', required=True, help='WDS Collection ID')
    parser.add_argument('-debug', dest="log_level", help="Set DEBUG logging level", action="store_const",
                        const=logging.DEBUG, default=logging.INFO)

    args = parser.parse_args()

    started_time = time.time()
    LOGGER = initialize_logger(args.log_level, os.path.basename(__file__))
    LOGGER.info("Starting discovery file upload script......")
    upload_driver(args)
    elapsed = time.time() - started_time
    LOGGER.info("Finished discovery file upload script. Elapsed time: %f" % elapsed)
