# -*- coding: utf-8 -*-
"""
Created on Tue Aug 23 11:03:50 2022

@author: Francisco Carrera
deal_hubspot_utils.py
"""

# Import libraries
import re
import pandas as pd
import numpy as np
import requests
import json
import time
import urllib
import tqdm

def hubspot_deal_note_extract(key):
    
    # Gather the notes associated with deals
    count = 100
    property_list = ['hs_object_id', 'hs_note_body']
    get_all_notes_url = "https://api.hubapi.com/crm/v3/objects/notes?"
    parameter_dict = {'limit': count, "associations":"deals,contacts", "archived":"false"}
    header = {'accept': 'application/json', 'Authorization': 'Bearer ' +  key}

    # Paginate your request using the after parameter
    after = "Value"
    note_df_master = pd.DataFrame()
    while after != None:
        parameters = urllib.parse.urlencode(parameter_dict)
        for prop in property_list:
            parameters = parameters + '&properties=' + prop
        get_url = get_all_notes_url + parameters
        r = requests.get(url= get_url, headers = header)
        response_dict = json.loads(r.text)
        # Process the note results
        results_dict = response_dict["results"]     
        results_dict_df = pd.DataFrame(results_dict)
        results_dict_df["Note"] = [prop["hs_note_body"] for prop in results_dict_df.properties]
        # Remove notes not associated with a deal
        results_dict_df.dropna(subset = "associations", inplace = True)
        results_dict_df["Contact_id"] = [prop["contacts"]["results"][0]["id"] if "contacts" in prop.keys() else np.nan \
                                         for prop in results_dict_df.associations]
        results_dict_df["Deal_id"] = [prop["deals"]["results"][0]["id"] if "deals" in prop.keys() else np.nan \
                                      for prop in results_dict_df.associations]
        # subset and append to master
        results_dict_df = results_dict_df.loc[:, ["id","Note","Deal_id","Contact_id"]]
        note_df_master = pd.concat([note_df_master,results_dict_df], ignore_index = True)
        # Paginate next call until all is exhausted
        try:
            after = response_dict['paging']["next"]["after"]
            parameter_dict["after"] = after
        except:
            after = None
        time.sleep(1)
    print('loop finished')
    # Return    
    return note_df_master

def hubspot_deal_details(key):
    
    # Gather the notes associated with deals
    count = 100
    property_list = ['dealname']
    get_all_notes_url = "https://api.hubapi.com/crm/v3/objects/deals?"
    parameter_dict = {'limit': count, "archived":"false"}
    header = {'accept': 'application/json', 'Authorization': 'Bearer ' +  key}

    # Paginate your request using the after parameter
    after = "Value"
    deal_df_master = pd.DataFrame()
    while after != None:
        parameters = urllib.parse.urlencode(parameter_dict)
        for prop in property_list:
            parameters = parameters + '&properties=' + prop
        get_url = get_all_notes_url + parameters
        r = requests.get(url= get_url, headers = header)
        response_dict = json.loads(r.text)
        # Process the note results
        results_dict = response_dict["results"]     
        results_dict_df = pd.DataFrame(results_dict)
        results_dict_df["Name"] = [prop["dealname"] for prop in results_dict_df.properties]
        # subset and append to master
        results_dict_df = results_dict_df.loc[:, ["id","Name"]]
        deal_df_master = pd.concat([deal_df_master,results_dict_df], ignore_index = True)
        # Paginate next call until all is exhausted
        try:
            after = response_dict['paging']["next"]["after"]
            parameter_dict["after"] = after
        except:
            after = None
        time.sleep(1)
    print('loop finished')
    # Return    
    return deal_df_master

def hubspot_contact_details(key):
    
    # Gather the notes associated with deals
    count = 100
    property_list = ['firstname','lastname','memory_care_needed',"mobility","taking_medication_", "bathing_assistance_needed_",
                     "desired_location_s__multipicklist","family_referred_to",'specified_budget']
    get_all_notes_url = "https://api.hubapi.com/crm/v3/objects/contacts?"
    parameter_dict = {'limit': count, "archived":"false"}
    header = {'accept': 'application/json', 'Authorization': 'Bearer ' +  key}

    # Paginate your request using the after parameter
    after = "Value"
    contact_df_master = pd.DataFrame()
    while after != None:
        parameters = urllib.parse.urlencode(parameter_dict)
        for prop in property_list:
            parameters = parameters + '&properties=' + prop
        get_url = get_all_notes_url + parameters
        r = requests.get(url= get_url, headers = header)
        response_dict = json.loads(r.text)
        # Process the note results
        results_dict = response_dict["results"]     
        results_dict_df = pd.DataFrame(results_dict)
        results_dict_df["Name"] = [prop["firstname"] + " " + prop["lastname"] \
                                   if prop["firstname"] != None and prop["lastname"] != None \
                                   else np.nan for prop in results_dict_df.properties]
        # subset and append to master
        results_dict_df = results_dict_df.loc[:, ["id","Name",'properties']]
        contact_df_master = pd.concat([contact_df_master,results_dict_df], ignore_index = True)
        # Paginate next call until all is exhausted
        try:
            after = response_dict['paging']["next"]["after"]
            parameter_dict["after"] = after
        except:
            after = None
        time.sleep(1)
    print('loop finished')
    # Return    
    return contact_df_master
