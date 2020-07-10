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
def upload_file(client, project_id, file_path):
    """ Upload file to project """
    client.post_doc_upload(project_id, "csv", os.path.basename(file_path), os.path.dirname(file_path))

        
## list all documents and return as pandas
def docs_df(client, project, limit, offset):
    
    ## get the document list
    doc_dict = doccano_client.get_document_list(project_id = 1)
    doc_count = doc_dict['count']
    nxt_page = doc_dict['next']
    docs_raw  = doc_dict['results']
    ## list documents as pandas

# ....