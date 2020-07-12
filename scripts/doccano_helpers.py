#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul  9 14:53:16 2020

@author: jmr
"""
from doccano_api_client import DoccanoClient
import os
import pandas as pd
from pandas.io.json import json_normalize
import ast
import re
import os

#### Main functions
## log in to the deccano's api 
def log_in(baseurl, username, pswrd_path):
    """instantiate a client and log in"""
    # instantiate a client and log in to a Doccano instance
    pswrd = open(pswrd_path, 'r').read().splitlines()[0]
    # instantiate a client and log in to a Doccano instance
    doccano_client = DoccanoClient(
        baseurl,
        username,
        pswrd 
    )
    ## check if all went well
    if len(doccano_client.get_me()) == 6:
        print("success!")
    else:
        print("login failed")
    return doccano_client

## labels_df
def labels_df(client, project_id):
    """get labels info as pandas df"""
    return json_normalize(client.get_label_list(project_id = project_id))

## Delete a label
def delete_label(client, project_id, label_id):
    """http DELETE request given project and label id"""
    client.session.delete(client.baseurl + "v1/projects/{project_id}/labels/{label_id}".format(project_id = project_id, label_id = label_id))

## uplad file
def upload_file(client, project_id, file_path, is_labeled = False):
    """ 
    Upload file(s) to project.
    If is_labeled = False and file_format = csv, it will erase all labels which are not equal to the existing ones before the calling.
    Rationale being that in the csv cases we are requested to provide a label.
    """
    ## retrieve existing labels before the upload
    existing_labels = labels_df(client, 1)['text'].tolist()
    ## get the file format
    ## several docs
    if isinstance(file_path, list):
        ## add several individual files
        for cur_path in file_path:
            try:
                file_format = re.compile("(?<=\\.).+?$").findall(cur_path)
                client.post_doc_upload(project_id, file_format[0], os.path.basename(cur_path), os.path.dirname(cur_path))
                print(cur_path + " uploaded!\n")
            except:
                pass
    else:
        # single doc        
        try:        
            file_format = re.compile("(?<=\\.).+?$").findall(file_path)
            client.post_doc_upload(project_id, file_format[0], os.path.basename(file_path), os.path.dirname(file_path))
        except:
            pass
        
    if file_format[0] == "csv" and is_labeled == False:
        current_labels = labels_df(client, 1)
        if len(current_labels['text'].tolist()) > len(existing_labels):
            ## remove the additional label
            label_id = current_labels.id[~current_labels.text.isin(existing_labels)].iloc[0]
            delete_label(client, project_id = 1, label_id = label_id)    
            current_labels = labels_df(client, 1)
            if len(current_labels['text'].tolist()) > len(existing_labels):
                print("Additional label present, double-check! double-check if on purpose")
            
### pull_docs
## its helpers
# flatten metadata
def flatten_listOfDicts(normalized_json, var, key):
    """flatten a list of dictionaries as """
    df_list = []
    for index, row in normalized_json.iterrows():
        # turn to dict
        if type(row[var]) == str:
            meta_dict = ast.literal_eval(row[var])
        elif len(row[var]) > 0:
            meta_dict = row[var]
        else:
            meta_dict = None
        
        if meta_dict != None:
            ## turn to df
            cur_df = pd.DataFrame(meta_dict, index = [0])
            ## add id var for 
            cur_df[key] = row[key]
        else:
            cur_df = pd.DataFrame([[None, row[key]]], columns = [var, key])
        df_list.append(cur_df)
    ## all in one and return
    return pd.concat(df_list)

# turn decanno list of docs into pandas
def doccano2pandas(docs_raw):
    """get the json file with the docs, flatten, and turn to pandas df"""
    ##  flatten the annotations and meta collumns
    flat_anno = flatten_listOfDicts(docs_raw, "annotations", "id")
    flat_meta = flatten_listOfDicts(docs_raw, "meta", "id")
    # left merge the flattened dfs
    first_merge = flat_meta.merge(flat_anno, on = "id", how = "left")
    ## left join it with the main df
    final_merge = docs_raw.drop(columns = ['annotations', "meta"]).merge(first_merge, on = "id", how = "left")        
    return final_merge

## main: pull_all_docs
def pull_all_docs(client, project_id):
    """
    Pull all docs from doccano. Tries to get all in one, if failing uses pagination.
    Finally, it flattens the dictionary and turns it into a pandas df.
    """
    # sequentially retrieve it using pagination
    ## starting values
    offset = 0
    limit = 1000
    df = pd.DataFrame()
    next_page = "fooh"
    while next_page:
        ## pull
        doc_dict = client.exp_get_doc_list(project_id = 1, limit = limit, offset = offset)
        # parse the results
        docs_raw  = json_normalize(doc_dict['results'])
        ## cleaning it up
        cleaned = doccano2pandas(docs_raw = docs_raw)
        ## get the data and row bind it
        df = pd.concat([df, cleaned])
        ## get the next page, existing...
        try:
            next_page = doc_dict['next']
        except:
            pass
        if isinstance(next_page, str):
            # prep the next limit and offset
            pattern_offset = re.compile("(?<=offset\=).+?$")
            offset = pattern_offset.findall(next_page)[0]    
            print("pulling docs from " + next_page + "\n")                      
    return df

## get labeled docs
def get_labeled_docs(client, project_id):
    ## download the csv containing the labeled docs
    resp = client.get_doc_download(1, "csv")
    # raise error depending on http response
    resp.raise_for_status()
    ## parse csv into pandas
    parsed = pd.read_csv(io.StringIO(resp.text))
    if parsed == "pandas.core.frame.DataFrame":
        return parsed
    else:
        return "no data retrieved"

## delete documents
def delete_docs(client, project_id, document_id, delete_all = False):
    """delete a document from doccanos database. 
        * document id can be either one document id (str) or several (list)
    """
    # delete all docs
    if delete_all:
        if isinstance(document_id, list) == False:
            raise Exception('For deleting all docs, you need to provide a list of docs as integers')
        # confirm with user input
        answer = input("Are you sure you want to permanently delete all docs?[y/n] ")
        if answer == "y":
            # start the loop
            for doc_id in document_id:
                # delete it
                print("deleting doc: " + str(doc_id))
                client.delete_document(project_id = 1, document_id = doc_id)
    # delete a specific doc
    else:
        client.delete_document(project_id = 1, document_id = document_id)

## annotate_docs
def annotate_docs(client, project_id, label_id, document_id):
    """
    annotate documents. 
        * document id can be either one document id (str) or several (list)
        * label id can be either one label id (str) or several (list). To get label ids, see "labels_df()"
    """
    if isinstance(document_id, str):
        # annotate
        client.add_annotation(project_id = project_id, annotation_id = label_id, document_id = document_id)
    elif isinstance(document_id, list):
        ## varying labels
        if isinstance(label_id, list) and len(set(label_id)) > 1:
            # start the loop
            for doc, label in zip(document_id, label_id):
                # annotate
                client.add_annotation(project_id = project_id, annotation_id = label, document_id = doc)
        ## all labels are the same
        else:
            ## start the loop
            for doc in document_id:
                # annotate
                client.add_annotation(project_id = project_id, annotation_id = label_id, document_id = doc)         
    else:
        raise TypeError('you need at least one valid document id or a list of valid document ids')

