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
import numpy as np


#### Main functions
## log in to the deccano's api 
def log_in(username = 'josemreis', pswrd_path = "/home/jmr/Desktop/decanno_access.txt"):
    """instantiate a client and log in"""
    # instantiate a client and log in to a Doccano instance
    pswrd = open(pswrd_path, 'r').read().splitlines()[0]
    # instantiate a client and log in to a Doccano instance
    doccano_client = DoccanoClient(
        'https://label-ecthr.herokuapp.com',
        username,
        pswrd 
    )
    ## check if all went well
    if len(doccano_client.get_me()) == 6:
        print("success!")
    else:
        print("login failed")
    return doccano_client

## uplad file
def upload_file(client, project_id, file_path, file_format = "json"):
    """ Upload file(s) to project """
    client.post_doc_upload(project_id, file_format, os.path.basename(file_path), os.path.dirname(file_path))

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
            df_list.append(cur_df)
    ## all in one, add prefix to cols, and return
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

## main: pull docs
def pull_docs(client, project, limit, offset, just_labeled = False):
    """
    Pull all docs from doccano. Tries to get all in one, if failing uses pagination.
    """
    # quickly get a doc count
    docs_all = client.get_document_list(project_id = 1)['count']
    print("\n>> Pulling " + str(docs_all) + " docs from docanno\n\n")
    ## pull all in one
    doc_dict = client.exp_get_doc_list(project_id = 1, limit = docs_all, offset = 0)
    # check if we retrieved all
    cur_counts = doc_dict['count'] 
    next_page = doc_dict['next']
    docs_raw  = json_normalize(doc_dict['results'])
    ## if there are still docs missing, limit might be too large
    # sequentially retrieve it using pagination
    if cur_counts != docs_all and next_page != None:
        ## starting values
        offset = 0
        limit = 1000
        df = pd.DataFrame()
        while next_page != None:
            ## pull
            doc_dict = client.exp_get_doc_list(project_id = 1, limit = limit, offset = offset)
            # check if we retrieved all
            cur_counts = doc_dict['count'] 
            next_page = doc_dict['next']
            docs_raw  = json_normalize(doc_dict['results'])
            ## get the data and row bind it
            df = pd.concat([df, json_normalize(doc_dict['results'])])
            # prep the next limit and offset
            pattern_limit = re.compile("(?<=limit=).+?(?=&)")
            limit = pattern_limit.findall(next_page)[0]
            pattern_offset = re.compile("(?<=offset=).+?$")
            offset = pattern_limit.findall(next_page)[0]                        
    ## wrangle
    # flattening and tidying
    final_df = doccano2pandas(docs_raw = docs_raw)
    if just_labeled:
        out = final_df[final_df.label.notnull()]
    else:
        out = final_df
    return out
    
