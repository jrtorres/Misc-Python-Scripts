import json
import csv
import datetime
import os
from os import listdir
from os.path import isfile, join
import sys
import argparse

timestamp = datetime.datetime.now().strftime("%Y.%m.%d-%H.%M.%S")

def write_dict_to_file(content_dict,out_f_path):
    try:
        with open(out_f_path, "w") as out_csv_file:
            csv_writer = csv.writer(out_csv_file)
            for dict_key, dict_values in content_dict.items():
                for dict_value in dict_values:
                    if isinstance(dict_value, list):
                        dict_value.insert(0,dict_key)
                        row = [i.encode('utf-8') for i in dict_value]
                        csv_writer.writerow(row)
                    else:
                        csv_writer.writerow([dict_key,dict_value])
        print "Completed writing to file: ", out_f_path
    except Exception as e:
        print "Error: Exception writing to file: ", str(e)

def extract_entities(file_name):
    entities_dict = {}
    with open(file_name, 'r') as json_file:
        full_workspace = json.load(json_file)
        entities = full_workspace['entities']
        print "\t", "Number of Entities:", len(entities)
        for entity in entities:
            entity_values = []
            entity_name = entity['entity']
            for entity_value in entity['values']:
                tmp_vals_list = []
                tmp_vals_list.append(entity_value['value'])
                for synonym in entity_value['synonyms']:
                    tmp_vals_list.append(synonym)
                entity_values.append(tmp_vals_list)
            print "\tEntity: %s has %d values" % (entity_name, len(entity['values']))
            entities_dict[entity_name] = entity_values
    return entities_dict

def extract_intents(file_name):
    intents_dict = {}
    with open(file_name, 'r') as json_file:
        full_workspace = json.load(json_file)
        intents = full_workspace['intents']
        print "\t", "Number of Intents:", len(intents)
        for intent in intents:
            intents_paraphrases = []
            paraphrase_counter = 0
            for paraphrase in intent['examples']:
                paraphrase_counter += 1
                intents_paraphrases.append(paraphrase['text'])
            print "\tIntent group: %s has %d paraphrases" % (intent['intent'], paraphrase_counter)
            intents_dict[intent['intent']] = intents_paraphrases
    return intents_dict

def parse_workspace_file(extraction_type, file_name):
    if isfile(file_name):
        f_ext_index = file_name.rfind(".")
        f_ext = file_name[f_ext_index:len(file_name)]
        if f_ext.find(".JSON") == 0 or f_ext.find(".json") == 0:
            f_name_prefix = file_name[0:f_ext_index]
            print "Parsing Workspace JSON file: ", file_name
            extracted_dict = {}
            print extraction_type.upper(), " extraction requested."
            if extraction_type == 'intents':
                extracted_dict = extract_intents(file_name)
            elif extraction_type == 'entities':
                extracted_dict = extract_entities(file_name)
            else:
                print "Unsupported extraction requested."

            if bool(extracted_dict):
                out_file_name = f_name_prefix + "_" + str(timestamp) + "_" + extraction_type + ".csv"
                write_dict_to_file(extracted_dict, out_file_name)
            else:
                print "Error: Empty extraction."
        else:
            print "Error: File does not have a JSON extension."
    else:
        print "Error: This is not a valid file name",file_name


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('extractiontype', action='store', choices=['intents', 'entities'], help="Type of extraction to perform. Specify intents or entities")
    parser.add_argument('filename', help="Workspace fully qualified file name")
    args = parser.parse_args()
    parse_workspace_file(args.extractiontype, args.filename)
